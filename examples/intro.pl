use Search::Brick::RAM::Query qw(query true false);
use Data::MessagePack;
use Data::Dumper;

sub __store_example {
    my $doc = 
    my $packer = Data::MessagePack->new();
    my $n = 9999;

    my @list = ();
    for (1..$n) {
        my $g = int(rand() * 100) % 20;
        push @list,{
            "title"   => "book",
            "year"    => 2014,
            "f_boost" => 3.2,
#            "author"  => "john",
#            "isbn"    => "239847982374",
#            "_data"   => "[1,2,3,4,5,6]",
#            "i_day"   => "7",
#            "f_day"   => "0.5",
#            "d_day"   => "0.7492",
#            "l_day"   => "10238123",
            "group_by" => "$g"
        }
    }
    @list = sort { $a->{group_by} <=> $b->{group_by} } @list;
    my $settings = {
        mapping => {
            title => {
                type  => "string",
                index =>  Data::MessagePack::true(),
                store =>  Data::MessagePack::true(),
            },
            f_boost => {
                type  => "float",
                index =>  Data::MessagePack::true()
            },
            year => {
                type  => "int",
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
            search_thread_pool_size => 15,
            expected => 0+ $n,
            shards => 6
        }
    };
    return query(timeout => 1_000_000,
                 brick   => 'RAM',
                 action  => 'store', 
                 index   => 'default',
                 settings => $settings,
                 request => \@list);
}

print Dumper([
    [ __store_example() ],
    [query(
         index => 'default',
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
         settings => { "dump_query" => true() } 
     )],
    [query(
         index => 'default',
         brick => 'RAM',
         action => 'search',
         timeout => 10,
         settings => { "dump_query" => true() , similarity => { "CustomSimilarity" => { }} },
         request => {constant_score => { query => { "term" => { "title" => "book" } }, boost => 9293.4 }}
     )],
    [query(
         index => 'default',
         brick => 'RAM',
         action => 'search',
         timeout => 10,
         settings => { "explain" => true(), dump_query => true() },
         request => {"term" => { "title" => "book" } }
     )],
     [query(
          index => 'default',
          brick => 'RAM',
          action => 'stat',
          timeout => 10,
      )
     ]]);
