use strict;
use warnings;
use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query qw(true);
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

my $b = Search::Brick::RAM::Query->new(index => '__test__');
$b->delete();

my $settings = {
    mapping => {
        author => {
            type  => "string",
            index =>  true(),
            store =>  true(),
            store_term_vector_positions => true(),
            store_term_vector_offsets => true(),
            store_term_vectors => true()
        },
        group_by => {
            type  => "string",
            index =>  true(),
            store =>  true()
        }
    },
    settings => {
        expected => 5,
        shards => 1,
    }
};
$b->index(
    [
     { author => 'jack b doe go!', group_by => "23"},
     { author => 'jack doe', group_by => "24"},
     { author => 'jack doe go!', group_by => "24"},
     { author => 'jack doe b go!', group_by => "24"},
     { author => 'doe jack b go!', group_by => "25"}
    ],$settings);

my @result = $b->search({ span_near => { clauses => [ { span_term => { author => 'jack' } }, { span_term => { author => 'doe' }} ], slop => 0, in_order => true() }});
is (@{ $result[0]->{hits} },1);
is ($result[0]->{hits}->[0]->{author},'jack doe');

@result = $b->search({ span_near => { clauses => [ { span_term => { author => 'jack' } }, { span_term => { author => 'doe' }} ], slop => 1 }});
is (@{ $result[0]->{hits} },2);


@result = $b->search(
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
    );
is (@{ $result[0]->{hits} },2);


@result = $b->search({ span_first => { match => { span_term => { author => "doe" }}, end => 1 }});
is (@{ $result[0]->{hits} },1);
is ($result[0]->{hits}->[0]->{author},'doe jack b go!');
done_testing();
