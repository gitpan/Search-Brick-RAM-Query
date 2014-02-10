use Search::Brick::RAM::Query qw(true);
use List::Util qw(shuffle);
use Data::Dumper;
use Elasticsearch;
use Elasticsearch::Bulk;
use Benchmark ':all';
my $e = Elasticsearch->new();
my $br = Search::Brick::RAM::Query->new(index => '__bench__test__');

my @words = grep { $_ } split(/[^\p{L}]/,q{
Lorem ipsum dolor sit amet, consectetur adipiscing elit. Pellentesque id sagittis massa, sed sollicitudin nulla. Suspendisse posuere justo odio, eu mollis orci viverra non. Nunc tincidunt accumsan ultricies. Vivamus tortor erat, mollis euismod sodales non, tristique euismod eros. Suspendisse non ligula sit amet odio bibendum pulvinar nec in nisl. Vestibulum varius, justo id suscipit posuere, turpis mauris dapibus erat, in facilisis velit dui sodales nibh. Aliquam vulputate, metus a adipiscing luctus, sapien ipsum fermentum nulla, sit amet tincidunt urna nisi at ipsum. Vivamus bibendum placerat condimentum. In hac habitasse platea dictumst. Aenean mauris mauris, convallis ut lectus sed, dapibus mattis nisl. Praesent semper ligula posuere sodales tristique. Etiam sed elit nisl. Cras suscipit, ante ut convallis malesuada, justo elit iaculis arcu, scelerisque tristique lacus libero vel leo. Aenean ac rutrum orci. Integer faucibus est vitae diam facilisis, non luctus leo congue.
});

sub sentence {
    my @x = ();
    for (1..rand(5)) {
        push @x,$words[rand(@words)];
    }
    return join(" ",@x);
}

sub document {
    my $g = shift || die 'need group';
    return {
        group_by => "$g",
        f_boost => rand(),
        author => sentence(),
        title  => sentence(),
        year   => int(rand(1000)),
        isbn   => int(rand(10000000))
    }
}

sub insert_into_brick {
    my $documents = shift || die 'docs';
    $br->delete();

    my $settings = {
        mapping => {
            author => {
                type  => "string",
                index =>  true(),
                store =>  true(),
            },
            f_boost => {
                type  =>  "float",
                index =>  true(),
                store =>  true(),
            },
            group_by => {
                type  => "string",
                index =>  true(),
                store =>  true()
            },
            title => {
                type  => "string",
                index =>  true(),
                store =>  true()
            },
            year => {
                type  => "int",
                index =>  true(),
                store =>  true()
            },
            isbn => {
                type  => "int",
                index =>  true(),
                store =>  true()
            }
        },
        settings => {
            expected => 0,
            shards => 2
        }
    };
    $settings->{settings}->{expected} = scalar(@{ $documents });
    $br->index($documents,$settings);
}

sub insert_into_es {
    my $documents = shift || die 'docs';
    $e->indices->delete(index => my_index);
    $e->indices->create(
        index      => 'my_index',
        body => {
            "settings" => {
                number_of_shards => 2,
                number_of_replicas => 0,
                "analysis" => {
                    "analyzer" => {
                        "whitespace" => {
                            "tokenizer" => "whitespace",
                            "filter"    => []
                        },
                    }
                }
            },
            "mapping" => {
                "my_type" => {
                    properties => {
                        "author"=> {
                            "index"=> "analyzed",
                            "type"=> "string",
                            "analyzer"=> "whitespace",
                            "store"=> "yes"
                        },
                        "title" => {
                            "index" => "analyzed",
                            "type"=> "string",
                            "analyzer"=> "whitespace",
                            "store"=> "yes"
                        },
                        "year" => {
                            "index" => "not_analyzed",
                            "type"=> "integer",
                            "store"=> "yes"
                        },
                        "group_by" => {
                             "index" => "not_analyzed",
                             "type"=> "string",
                             "store"=> "yes"
                         },
                        "isbn" => {
                            "index" => "not_analyzed",
                            "type"=> "integer",
                            "store"=> "yes"
                        },
                        "f_boost" => {
                            "index" => "not_analyzed",
                            "type"=> "float",
                            "store"=> "yes"
                        },
                    }
                }
            }
        }
        );


    my $bulk = Elasticsearch::Bulk->new(
        es      => $e,
        index   => 'my_index',
        type    => 'my_type',
        max_count   => 0
        );

    $bulk->create_docs(@{ $documents });
    $bulk->flush();
    $e->indices->optimize(index => 'my_index');
}

sub search_brick {
    $br->search({ term => { author => "dolor" } },{ size => 5 });
}

sub search_es {
    return $e->search(
        index => 'my_index',
        body  => {
            query => {
                term => { author => 'dolor' }
            },
            size => 5
        }
    );
}

my @documents = ();
for (1..200_000) {
    my $doc = document($_);
    push @documents,$doc;
}

cmpthese(1,{
    'b' => sub {
        insert_into_brick(\@documents);
    },
    'e' => sub {
        insert_into_es(\@documents);
    }
});

print Dumper({ b =>[ search_brick() ], e => search_es() });

cmpthese(100_000,{
    'b-search' => sub {
        search_brick();
    },
    'e-search' => sub {
        search_es();
    }
});
