use strict;
use warnings;

use Test::More;
use Test::Exception;
use Search::Brick::RAM::Query;
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

my $b = Search::Brick::RAM::Query->new(index => '__test__');
$b->delete();
my $settings = {
    mapping => {
        author => {
            type  => "string",
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

$b->index([{ author => 'jack', group_by => "23" },{ author => 'john', group_by => "24" }],$settings);
my @result = $b->search({ match_all => {} });
is(scalar(@{ $result[0]->{hits} }),2);
for my $who(qw(jack john)) {
    for my $type(qw(filter cached_filter)) {
        my @result = $b->search({ filtered => { query => { match_all => {} }, $type => { term => { author => $who } } } },
                                { items_per_group => 2, size => 1});
        is(scalar(@{ $result[0]->{hits}}),1);
        is($result[0]->{hits}->[0]->{author},$who);
    }
}

done_testing();
