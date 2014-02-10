package Search::Brick::RAM::Query;
use Socket;
use POSIX;
use Data::Dumper;
use Data::MessagePack;
use Data::MessagePack::Stream;
use Time::HiRes qw(time);
use Carp;
use IO::Select;
use strict;
use warnings;
require Exporter;
use constant DEFAULT_TIMEOUT => 10;
my $clock;
eval {
    Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC());
    $clock = sub { Time::HiRes::clock_gettime(Time::HiRes::CLOCK_MONOTONIC()) };
    1;
} or do {
    $clock = sub { time() };
};

our @ISA = qw(Exporter);
our %EXPORT_TAGS = ( 'all' => [ qw() ] );
our @EXPORT_OK = ( @{ $EXPORT_TAGS{'all'} } );
our @EXPORT = qw(fetch query new tcp true false msgpack_array_begin msgpack_map_begin);
our $VERSION = '0.01';

# some parts are borrowed from Hijk
# https://github.com/gugod/Hijk/blob/master/lib/Hijk.pm
sub tcp {
    my ($host, $data,$timeout, $callback) = @_;
    $host = [ $host ] 
        unless ref($host) eq 'ARRAY';

    my $s = IO::Select->new();
    my $start = $clock->();
    my %sockets = ();

    for my $h(@{ $host }) {
        my ($hh,$pp) = split(/:/,$h);
        $pp ||= 9000;
        my $fd;
        socket($fd, PF_INET, SOCK_STREAM, getprotobyname('tcp')) || die "<$host> failed to construct TCP socket, errno($?) = $!";
        my $flags = fcntl($fd, F_GETFL, 0) or die "<$host> failed to set fcntl F_GETFL flag for socket, errno($?) = $!";
        fcntl($fd, F_SETFL, $flags | O_NONBLOCK) or die "<$host> failed to set socket to non_blocking, errno($?) = $!";
        connect($fd, sockaddr_in($pp, inet_aton($hh))) or do {
            die "<$h> connect(2) error, errno($?) = $!"
                if ($! != EINPROGRESS);
        };
        $sockets{$fd} = { data =>  $data, total => length($data), left => length($data), info => $h, handle => $fd };
        $s->add($fd);
    }

    while ($s->count() > 0) {
        my @ready = $s->can_write(__timed_out($start,$timeout));
        croak "write error, timeout = $timeout, errno($?) = $!"
            unless scalar(@ready) > 0;
        for my $rfd(@ready) {
            my $r =  $sockets{$rfd};
            my $n = syswrite($rfd,$data,$r->{left}, $r->{total} -  $r->{left});
            unless($n) {
                next
                    if ($! == POSIX::EWOULDBLOCK || $! == POSIX::EAGAIN);
                die "<$r->{info}> write error, errno($?)= $!";
            }
            $r->{left} -= $n;
            if ($r->{left} <= 0) {
                $s->remove($rfd);
            }
        }
    }

    for my $r(values(%sockets)) {
        $s->add($r->{handle});
    }

    while($s->count() > 0) {
        my @ready = $s->can_read(__timed_out($start,$timeout));
        croak "read error, timeout = $timeout, errno($?) = $!"
            unless (scalar(@ready) > 0);
        for my $rfd(@ready) {
            my $r =  $sockets{$rfd};
            my $nbytes = sysread($rfd, my $buf, 10204);
            if (!$nbytes) {
                die "<$r->{info}> socket closed while still trying to read, errno = $!"
                    if !defined($nbytes) || ($nbytes == 0);
                die "<$r->{info}> read error, errno($?) = $!"
                    if ($nbytes == -1 && ($! != POSIX::EAGAIN || $! != POSIX::EWOULDBLOCK));
                next;
            }
            if ($callback->($r,$buf,$nbytes) == 1) {
                $s->remove($rfd);
                shutdown($rfd,SHUT_RDWR);
                close($rfd);
            }
        }
    }
}

sub true {
    Data::MessagePack::true();
}
sub false {
    Data::MessagePack::false();
}

sub fetch {
    my %params = @_;
    my $host       = $params{host}        || ['127.0.0.1:9000'];
    my $request    = $params{request}     || undef;
    my $settings   = $params{settings}    || {};
    my $timeout    = $params{timeout}     || DEFAULT_TIMEOUT();
    my $dispatcher = $params{dispatcher}  || die 'need dispatcher';

    my $packer     = Data::MessagePack->new();
    $request = ref($request) ?
                  $packer->pack($request)
                : defined($request) ? $request
                : $packer->pack(undef);

    my $packed     = $packer->pack($dispatcher) .  $packer->pack($settings) . $request;
    my @results = ();
    tcp($host,$packed,$timeout,sub {
        my ($r,$buf) = @_;
        my $unpacker = $r->{__UNPACKER__} || ($r->{__UNPACKER__} = Data::MessagePack::Stream->new());
        $unpacker->feed($buf);
        if ($unpacker->next) {
            my $result = $unpacker->data;
            croak "<$r->{info}> failed to read, expected ref() got scalar: " . ($result || 'undefined result')
                unless ref($result);
            push @results,$result;
            return 1;
        }
        return 0;
    });
    return @results;
}

sub __timed_out {
    my ($start,$timeout) = @_;
    my $diff = ($clock->() - $start);
    croak "timeout $timeout",
        if ($timeout < $diff);
    return $timeout - $diff;
}

sub query {
    my %params = @_;
    my $brick   = delete($params{brick})   || die 'need brick';
    my $index   = delete($params{index})   || die 'need index';
    my $action  = delete($params{action})  || 'search';
    $params{dispatcher} = join(":",$brick,$action,$index);
    return fetch(%params);
}

sub new {
    my $class = shift;
    my %params = @_;
    my $host  = $params{host}  || '127.0.0.1:9000';
    my $index = $params{index} || 'default';
    my $brick = $params{brick} || 'RAM';
    return bless { host => $host, index => $index, brick => $brick }, $class;
}

sub params {
    my ($self,$action,$timeout) = @_;
    return {
        index    => $self->{index},
        host     => $self->{host},
        brick    => $self->{brick},
        timeout  => $timeout || DEFAULT_TIMEOUT(),
        action   => $action
    }
}

sub search {
    my ($self,$request,$settings,$timeout) = @_;
    query(%{ $self->params('search',$timeout) },
          request  => $request,
          settings => $settings);
}

sub delete {
    my ($self,$timeout) = @_;
    query(%{ $self->params('delete',$timeout) });
}

sub stat {
    my ($self,$timeout) = @_;
    query(%{ $self->params('stat',$timeout) });
}

sub alias {
    my ($self,$request,$timeout) = @_;
    query(%{ $self->params('alias',$timeout) }, settings => $request);
}

sub index {
    my ($self,$request,$settings,$timeout) = @_;
    query(
        %{ $self->params('store',$timeout) },
        request  => $request,
        settings => $settings);
}


# some encode helper functions (in case you want to encode huge arrays or maps)
# it is easier to just join msgpacked() bytes together, and then prepend the actual msgpack_array_begin
sub msgpack_array_begin {
    # taken from https://github.com/msgpack/msgpack-perl/blob/master/lib/Data/MessagePack/PP.pm#L199
    my $num = shift;
    my $header =
                $num < 16          ? CORE::pack( 'C',  0x90 + $num )
              : $num < 2 ** 16 - 1 ? CORE::pack( 'Cn', 0xdc,  $num )
              : $num < 2 ** 32 - 1 ? CORE::pack( 'CN', 0xdd,  $num )
              : die("number %d", $num)
              ;

    return $header;
}

sub msgpack_map_begin {
    # taken from https://github.com/msgpack/msgpack-perl/blob/master/lib/Data/MessagePack/PP.pm#L212
    my $num = shift;
    my $header =
          $num < 16          ? CORE::pack( 'C',  0x80 + $num )
        : $num < 2 ** 16 - 1 ? CORE::pack( 'Cn', 0xde,  $num )
        : $num < 2 ** 32 - 1 ? CORE::pack( 'CN', 0xdf,  $num )
        : die("number %d", $num)
        ;
    return $header;
}


1;
__END__
=head1 NAME

Search::Brick::RAM::Query - Perl interface for the RAMBrick search thingie
https://github.com/jackdoe/brick/tree/master/bricks/RAMBrick

=head1 SYNOPSIS

 use Search::Brick::RAM::Query qw(query true)
 my @results = query(host    => ['127.0.0.1:9000'],
                     settings => { log_slower_than => 100, explain => true }, # log queries slower than 100ms
                     request  => { term => { "title" => "book","author" => "john" } },
                     brick    => 'RAM',
                     action   => 'search',
                     timeout  => 0.5 # in seconds - this is total timeout
                                     # connect time + write time + read time
               );

 #shortcuts are available using the search object:
 my $s = Search::Brick::RAM::Query->new(host => '127.0.0.1:9000', index => '__test__');
 my @results = $s->search({ term => { author => 'jack' } }, { size => 20, explain => true, log_slower_than => 5 });
 $s->delete(); # deletes the __test__ index

=head1 DESCRIPTION

minimalistic interface to RAMBrick

=head1 FUNCTION: Search::Brick::RAM::Query::query( $args :Hash ) :Ref

C<Search::Brick::RAM::Query::query> has the following parameters:

 host    => ['127.0.0.1:9000'],
 request => { term => { "title" => "book","author" => "john" } },
 brick   => 'RAM',
 action  => 'search', # (search|store|alias|load)
 index   => '...',    # name of your index
 timeout => 0.5,      # in seconds
 settings => {}       # used by different actions to get context on the reques

C<timeout> is the whole round trip timeout: (connect time + write time + 
read time).

C<brick> is the brick's name (in this case 'RAM')

C<action> is the requested action (search|store|stat)

C<request> must be reference to array of hashrefs

C<index> is the action argument, provides context to the request (usually index name)

C<settings> C<RAMBrick> requires settings to be sent (by default they are empty) things like "size" or "items_per_group" like: C<< { size => 5, explain => true } >>

C<host> string or arrayref of strings - the same request will be sent to all hosts in the list (the whole thing is async, so will be as slow as the slowest host) and un-ordered array of results is returned.

If the result is not C<ref()> or there is any kind of error(including timeout),
the request will die.

=head2 SEARCH

 my $s = Search::Brick::RAM::Query->new(host => '127.0.0.1:9000', index => '__test__');
 my @results = $s->search({ term => { author => 'jack' } }, { size => 20, explain => true, log_slower_than => 5 });

=head3 QUERY SYNTAX

=over 2

=item boosting

every query except C<term> and C<lucene>  supports C<boost> parameter like:

 bool => { must [ { term => { author => 'jack' } } ], boost => 4.3 }

=item term

creates L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/TermQuery.html>

syntax:

 term => { field => value }

example:

  my @results = $s->search({ term => { author => 'jack' } });

since RAMBrick does not do any query rewrites (like ElasticSearch's C<match> query) and it also does not do any kind of analysis on the query string, the C<term> and C<lucene> queries are the only queries that can be used to match specific documents.

=item lucene

creates L<http://lucene.apache.org/core/4_6_0/queryparser/org/apache/lucene/queryparser/classic/package-summary.html#package_description> query:

syntax:

 lucene => "author:john"

example:

 my @result = $s->search({ lucene => "ConstantScore(author:john)^8273" });

you can do pretty much everything with it like in this example it creates a C<constant score query> over a C<term query>

=item dis_max

syntax:
  
 dis_max => {
     queries => [ 
         { term => { author => 'jack' } },
         { term => { category => 'comedy' } } 
     ],
     boost => 1.0,
     tie_breaker => 0.3
 }


=item bool

syntax:

 bool => {
     must => [
         { term => { author => 'jack' } },
     ],
     should => [
         { term => { category => 'drama' } },
         { term => { category => 'comedy' } }
     ],
     must_not => [
         { term => { deleted => 'true' } }
     ],
     minimum_should_match => 1,
     boost => 1.0
 }

=item constant_score

creates L<https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/ConstantScoreQuery.html>

syntax:

 constant_score => { query => ..., boost => 4.0 }

=item filtered

creates L<https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/QueryWrapperFilter.html>
or L<https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/CachingWrapperFilter.html>

syntax:

 filtered => { query => ..., filter => { query.. } }
 filtered => { query => ..., cached_filter => { query.. } }

example:

 $s->search(
 {
        filtered => { 
                 query => { match_all => {} },
                 cached_filter => { term => { author => "jack" } }
        } 
 })


=item match_all

creates L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/MatchAllDocsQuery.html>

syntax:

 match_all => {}

 
=item custom_score

syntax:

 { 
    custom_score => { 
        query => { 
            term => { author => "jack" } 
        }, 
        class => 'bz.brick.RAMBrick.BoosterQuery', 
        params => {} 
    }
 }

will create an instance of BoosterQuery, with "Map<String,Map<String,String>>" params

look at L<https://github.com/jackdoe/brick/blob/master/bricks/RAMBrick/queries/BoosterQuery.java> for simple example

=item span_first

creates L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/spans/SpanFirstQuery.html>

synax:

 { 
    span_first => { 
        match => { 
            span_term => { author => "doe" }
        }, 
        end => 1 
    }
 }

matches the term "doe" in the first "end" positions of the field
more detailed info on the span queries: L<http://searchhub.org/2009/07/18/the-spanquery/>

=item span_near

creates L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/spans/SpanNearQuery.html>

syntax:
 {
    span_near => { 
        clauses => [ 
            { span_term => { author => 'jack' } }, 
            { span_term => { author => 'doe' } } 
        ], 
        slop => 0,
        in_order => true()
    }
 }

more detailed info on the span queries: L<http://searchhub.org/2009/07/18/the-spanquery/>
example:

 {
    span_near => {
        clauses => [
            {
                span_near => {
                    clauses => [
                        { span_term => { author => 'jack' } },
                        { span_term => { author => 'doe' } }
                     ],
                     slop => 1
                },
            },
            {
                span_term => { author => 'go!' }
            }
        ],
        slop => 0
    }
 }


=item span_term

creates L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/spans/SpanTermQuery.html>

syntax:
 span_term => { field => value }

span_term queries are the building block of all span queries

=back

=head3 QUERY SETTINGS

=over 2

=item log_slower_than

syntax:

 log_slower_than => 5

example:

 my @result = $s->search({ term => { author => 'jack' } },{ log_slower_than => 5 });

if the query takes more than 5 milliseconds, in the return object there will be a C<query> key which will contain the actual query.
in this case it will look like this:

 {
   hits => [],
   took => 6,
   query => "author:jack"
 }

=item explain

syntax:
 explain => true()

example:
 my @result = $s->search({ term => { author => 'jack' } },{ explain => true() });

will fill '__explain' field in each document, like this:

 {
   hits => [
  {
 ...
 '__score' => '4.19903135299683',
 '__explain' => '4.1990314 = (MATCH) weight(author:jack in 19) [BoostSimilarity], result of:
  4.1990314 = <3.2> value of f_boost field + subscorer
    0.9990314 = score(doc=19,freq=1.0 = termFreq=1.0), product of:
      0.9995156 = queryWeight, product of:
        0.9995156 = idf(docFreq=2064, maxDocs=2064)
        1.0 = queryNorm
      0.9995156 = fieldWeight in 19, product of:
        1.0 = tf(freq=1.0), with freq of:
          1.0 = termFreq=1.0
        0.9995156 = idf(docFreq=2064, maxDocs=2064)
        1.0 = fieldNorm(doc=19)
 ',
 'author' => 'jack',
 ...
 }
   ],
   took => 6,
 }

syntax:
 dump_query => true()

example:
 my @result = $s->search({ term => { author => 'jack' } },{ dump_query => true() });

will return the actual query.toString() in the result structure:

 [
  {
    'took' => 109,
    'query' => 'author:jack',
    'hits' => [ {},{}... ]
  }
 ]


=back

=head2 INDEX

all the indexes are created from messagepack'ed streams, following the same protocol:

 mapping + settings
 data

example: 

 my $settings = {
    mapping => {
        author => {
            type  => "string", # "string|int|long|double|float",
            index =>  true(),
            store =>  true(),
            omit_norms => false(),
            store_term_vector_offsets => false(),
            store_term_vector_positions => false(),
            store_term_vector_payloads => false(),
            store_term_vectors => false(),
            tokenized => true()
        },
        group_by => {
            type  => "string",
            index =>  true(),
            store =>  false()
        }
    },
    settings => {
        expected => 2, # number of expected documents from the data-part
        shards => 4,   # how many shards will be created 
                       # (in this example each shard will have 1/4th of the data)
        similarity => "bz.brick.RAMBrick.IgnoreIDFSimilarity", # can use:
                                                               # org.apache.lucene.search.similarities.BM25Similarity
                                                               # org.apache.lucene.search.similarities.DefaultSimilarity
                                                               # etc..
        expect_array => true(),
        store => "/var/lib/brick/ram/primary_index_20022014" # it will actually create lucene index there
                                                             # and next time it tries to autload the file
                                                             # it will just check if number of documents
                                                             # match.
    }
 };

check out the field type options from: L<https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/document/FieldType.html>
similarity information: L<https://lucene.apache.org/core/4_6_0/core/org/apache/lucene/search/similarities/Similarity.html>

the "data" can be in array format, or just concatinated documnts joined by '' (depending on the "expect_array" setting)

=head4 there are 2 ways to index:

=over 2

=item

You can create .messagepack files by concatinating the settings with the data, and just putting it in the "RAMBRICK_AUTO_LOAD_ROOT" (by default "/var/lib/brick/ram/") directory (start brick with RAMBRICK_AUTO_LOAD_ROOT env variable set to wherever you want).

=item

or you can use the "store" action

 my $s = Search::Brick::RAM::Query->new(host => '127.0.0.1:9000', index => '__test__');
 $s->index([{ author => 'jack', group_by => "23" },{ author => 'jack', group_by => "24" }],$settings);

this will just send one blob of data to "ram:store:__test__", which will be rerouted to "RAMBrick.store('__test__',unpacker)"
and the next portion of data will be in the format <settings><data...>

=back

In case the number of expected documents does not match the number of documents indexed, it will not create the index.

=head4 the store option

there is an option to store the indexes on disk, just specify the directory name in the index's settings (it MUST be somewhere within the RAMBRICK_AUTO_LOAD_ROOT).

the structure in our $settings example will look like:

 /var/lib/brick/ram/primary_index_20022014/SHARD_0
 /var/lib/brick/ram/primary_index_20022014/SHARD_1
 /var/lib/brick/ram/primary_index_20022014/SHARD_2
 /var/lib/brick/ram/primary_index_20022014/SHARD_3

each of those directories will contain the lucene index (using L<http://lucene.apache.org/core/4_6_0/core/org/apache/lucene/store/NIOFSDirectory.html>)

=head4 delete an index

when you delete an index, it will delete the autoload .messagepack file + the stored directory

 my $s = Search::Brick::RAM::Query->new(host => '127.0.0.1:9000', index => '__test__');
 $s->delete();


=head2 ALIAS

=head2 STAT

 my $s = Search::Brick::RAM::Query->new(host => '127.0.0.1:9000', index => '__test__');
 print Dumper([$s->stat()]);
 
will produce:

 [
  {
   'index.default.groups' => 20
   'index.default.searches' => 4,
   'index.default.documents' => 9999,
   'index.default.last_query_stamp' => 1391416081,
   'java.non_heap.usage.used' => 12678192,
   'java.heap.usage.init' => 31138368,
   'java.heap.usage.committed' => 91226112,
   'java.heap.usage.used' => 47775336,
   'java.non_heap.usage.init' => 24576000,
   'java.heap.usage.max' => 620756992,
   'java.non_heap.usage.max' => 224395264,
   'java.non_heap.usage.committed' => 24576000,
   'main.connection_pool.size' => 12,
   'main.connection_pool.active' => 1,
   'brick.search_pool.size' => 15,
   'brick.search_pool.active' => 0,
   'brick.time_indexing' => 3,
   'brick.uptime' => 212,
   'brick.time_searching' => 0,
   'brick.searches' => 8,
  }
 ]


=head2 EXAMPLES:

at the moment it looks like this: sister queries are joined by BooleanQuery with a
MUST clause for example:

 { 
     term => { "author" => "john" },
     dis_max => { 
        queries => [ 
            { term => { "category" => "horror" } },
            { term => { "category" => "comedy" } } 
        ], 
        tie_breaker => 0.2 
     }
 },

will generate: C<+(category:horror | category:comedy)~0.2 +author:john>.
different example:

 query(
  host => ['127.0.0.1:9000','127.0.0.1:9000'],
  request => { 
     term => { "author" => "john" } ,
     dis_max => { 
        queries => [ 
         { term => { "category" => "horror" } },
         { term => { "category" => "comedy" } } 
        ], 
        tie_breaker => 0.2 
     },
     bool => {
         must => [
            { lucene => "isbn:74623783 AND year:[20021201 TO 200402302]" }
         ],
         should => [
            { lucene => "cover:(red OR blue)" },
            { term => { old => "0" } }
         ],
         must_not => [
            { term => { deleted => "1" } }
         ]
     }
  },
  brick => 'RAM',
  timeout => 10,
  settings => { "dump_query" => "true" });
  
generates: C<+((+(+isbn:74623783 +year:[20021201 TO 200402302]) -deleted:1 (cover:red cover:blue) old:0)~1) +(category:horror | category:comedy)~0.2 +author:john>

another example:

 my $b = Search::Brick::RAM::Query->new(index => '__test__');
 $b->delete();
 my $settings = {
    mapping => {
        author => {
            type  => "string",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true(),
        },
        f_boost => {
            type  =>  "float",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true(),
        },
        group_by => {
            type  => "string",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true()
        }
    },
    settings => {
        expected => 2,
        shards => 1
    }
 };

 $b->index([
     { author => 'jack', group_by => "23", f_boost => 0.5 },
     { author => 'john', group_by => "24",f_boost => 0.5 }
   ],$settings);
 my @result = $b->search({ lucene => "ConstantScore(author:john)^8273" },
                         { log_slower_than => "1",explain => "true" } );
 $VAR1 = [{
  'took' => 4,
  'hits' => [
    {
      '__score' => '8273.5',
      'f_boost' => '0.5',
      '__group_index' => '0',
      'group_by' => '24',
      '__explain' => '8273.5 = (MATCH) sum of:
  8273.5 = (MATCH) weight(author:john^8273.0 in 1) [BoostSimilarity], result of:
    8273.5 = <0.5> value of f_boost field + subscorer
      8273.0 = score(doc=1,freq=1.0 = termFreq=1.0), product of:
        8273.0 = queryWeight, product of:
          8273.0 = boost
          1.0 = idf(docFreq=1, maxDocs=2)
          1.0 = queryNorm
        1.0 = fieldWeight in 1, product of:
          1.0 = tf(freq=1.0), with freq of:
            1.0 = termFreq=1.0
          1.0 = idf(docFreq=1, maxDocs=2)
          1.0 = fieldNorm(doc=1)
',
      'author' => 'john'
    }
  ],
  'query' => '__no_default_field__:ConstantScore author:john^8273.0'
 }];

as you can see the return structure is [{},{},{}] one result per 
request (for example if we do $b = Search::Brick::RAM::Query->new(host => [ '127.0.0.1:900','127.0.0.1:900])
there will be [{hits => []},{hits => []}] in the output)

=head1 SEE ALSO

lucene: L<http://lucene.apache.org/core/4_6_0/>

brick: L<https://github.com/jackdoe/brick>

=head1 AUTHOR

Borislav Nikolov, E<lt>jack@sofialondonmoskva.comE<gt>

=head1 COPYRIGHT AND LICENSE

Copyright (C) 2014 by Borislav Nikolov

This library is free software; you can redistribute it and/or modify
it under the same terms as Perl itself, either Perl version 5.18.2 or,
at your option, any later version of Perl 5 you may have available.


=cut
