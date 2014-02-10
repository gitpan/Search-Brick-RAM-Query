use strict;
use warnings;

use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query;
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

my $br = Search::Brick::RAM::Query->new(index => '__test__');
$br->delete();
my $n = 100;
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
        },
        f_boost => {
            type  => "float",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true()
        },
    },
    settings => {
        expected => $n,
        shards => 5
    }
};


my @docs = ();
for (1..$n) {
    push @docs,{ author => 'jack', group_by => "$_" };
}
$br->index(\@docs,$settings);

for my $i(1..$n) {
    my @result = $br->search({ term => { author => "jack" }},{ items_per_group => 2, size => $i});
    is (scalar(@result),1);
    is (scalar(@{ $result[0]->{hits} }),$i);
    is ($result[0]->{hits}->[0]->{author},"jack");
}

$br->delete();

@docs = ();
for (1..$n) {
    my $g = $_ % 20;
    push @docs,{ author => 'jack', group_by => "$g" };
}
$docs[-1]->{f_boost} = 1000.0;
$docs[-1]->{author} = "jack bazinga";
$docs[-1]->{group_by} = "7";

$docs[-2]->{f_boost} = 500.0;
$docs[-2]->{author} = "jack no bazinga";
$docs[-2]->{group_by} = "6";

@docs = sort { $a->{group_by} cmp $b->{group_by} } @docs;
$br->index(\@docs,$settings);

my @result = $br->search({ custom_score => { query => { term => { author => "jack" } }, class => "bz.brick.RAMBrick.BoosterQuery", params => {}}},{ items_per_group => 10, size => 10});
is (scalar(@result),1);
is (scalar(@{ $result[0]->{hits} }),10);
is ($result[0]->{hits}->[0]->{author},"jack bazinga");
is ($result[0]->{hits}->[1]->{author},"jack no bazinga");
done_testing();
