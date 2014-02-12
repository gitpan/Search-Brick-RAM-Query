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
$b->index([{ author => 'jack', group_by => "23" },{ author => 'john', group_by => "24", }],$settings);
my @r = $b->alias({ add => [{ 'shiny_new_alias' => '__test__', 'wrong' => 'non_existing_index' }], delete => [ 'non_existing_alias'] });
is (scalar(grep { $_ =~ /missing alias <non_existing_alias>/ } @{ $r[0] }),1);
is (scalar(grep { $_ =~ /<non_existing_index>/ } @{ $r[0] }),1);
my @result = $b->search({ match_all => {} });
is(scalar(@{ $result[0]->{hits} }),2);

my $b_aliased = Search::Brick::RAM::Query->new(index => 'shiny_new_alias');

for my $who(qw(jack john)) {
    my @result = $b->search({ constant_score => { query => {term => {author => $who }}, boost => 8273.0 }},{ log_slower_than => 1,explain => true} );
    my @result_a = $b_aliased->search({ constant_score => { query => {term => {author => $who }}, boost => 8273.0 }},{ log_slower_than => 1,explain => true} );
    if (scalar(@result) == scalar(@result_a)) {
        $result_a[$_]->{took} = $result[$_]->{took} for (0..$#result);
    }
    is_deeply(\@result,\@result_a);
    is(scalar(@{ $result[0]->{hits}}),1);
    is($result[0]->{hits}->[0]->{author},$who);
    is($result[0]->{hits}->[0]->{__score},8273);
    like($result[0]->{hits}->[0]->{__explain},qr/8273/);
    like($result[0]->{query},qr/ConstantScore/);
}

@r = $b->alias({ delete => [ 'shiny_new_alias'] });
is (scalar(@{ $r[0] }),0);
throws_ok {
    my @result_a = $b_aliased->search({ constant_score => { query => {term => {author => 'jack' }}, boost => 8273.0 }},{ log_slower_than => 1,explain => true} );
} qr/<shiny_new_alias> index is not loaded yet/;

lives_ok {
    my @result = $b->search({ constant_score => { query => {term => {author => 'jack' }}, boost => 8273.0 }},{ log_slower_than => 1,explain => true} );
    is(scalar(@{ $result[0]->{hits}}),1);
};

done_testing();
