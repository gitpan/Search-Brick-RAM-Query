use strict;
use warnings;

use Test::More;
use Test::Exception;
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
        }
    },
    settings => {
        expected => 3,
        shards => 1,
    }
};
$b->index([{ author => 'jack', group_by => "23", f_boost => 0.5 },{ author => 'john', group_by => "24",f_boost => 0.5 },{ author => 'john', group_by => "25",f_boost => 0.5 }],$settings);

my @result = $b->search({ match_all => {} });
is(scalar(@{ $result[0]->{hits} }),3);
for my $who(qw(jack john)) {
    my @result = $b->search({ custom_score => { query => { lucene => "ConstantScore(author:$who)^8273" }, class => 'bz.brick.RAMBrick.BoosterQuery', params => {} }},{ log_slower_than => 1,explain => true() } );
    is(scalar(@{ $result[0]->{hits}}),($who eq 'john' ? 2 : 1));
    is($result[0]->{hits}->[0]->{author},$who);
    is($result[0]->{hits}->[0]->{__score},8273.5);
    like($result[0]->{hits}->[0]->{__explain},qr/8273.0/);
    like($result[0]->{query},qr/ConstantScore author/);
}

done_testing();
