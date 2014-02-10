use strict;
use warnings;
use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query qw(true);
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

for my $sim(qw(org.apache.lucene.search.similarities.BM25Similarity bz.brick.RAMBrick.IgnoreIDFSimilarity org.apache.lucene.search.similarities.DefaultSimilarity)) {
    my $b = Search::Brick::RAM::Query->new(index => '__test__');
    $b->delete();

    my $settings = {
        mapping => {
            author => {
                type  => "string",
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
            expected => 1,
            shards => 1,
            similarity => $sim,
        }
    };
    $b->index([{ author => 'jack', group_by => "23"}],$settings);

    my @result = $b->search({ term => { author => 'jack'} },{explain => true()});
    my $s = $sim;
    $s =~ s/.*?\.(\w+)$/$1/;
    like($result[0]->{hits}->[0]->{__explain},qr/\[$s\]/);
}
done_testing();
