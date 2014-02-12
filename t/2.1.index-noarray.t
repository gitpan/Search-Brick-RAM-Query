use strict;
use warnings;

use Test::More;
use Test::Exception;
use Search::Brick::RAM::Query qw(false true);
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

my $b = Search::Brick::RAM::Query->new(index => '__test__');
#$b->delete();
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
        },
        f_boost => {
            type  => "float",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true()
        }
    },
    settings => {
        expected => 2,
        shards => 1,
        expect_array => false()
    }
};
my $mp = Data::MessagePack->new();
my $packed = join("",$mp->pack({ author => 'jack', group_by => "23", f_boost => 0.5 }),$mp->pack({ author => 'jack', group_by => "24", f_boost => 0.5 }));
$b->index($packed,$settings);
my @result = $b->search({ term => { author => "jack" }},{ items_per_group => 2, size => 1});
throws_ok { 
    $b->search({ term => { author => "jack" }},{ items_per_group => 2, size => 1, no_parameter_defined => 3})
} qr/unknown parameter/i;
is (scalar(@result),1);
is (scalar(@{ $result[0]->{hits} }),1);
is ($result[0]->{hits}->[0]->{author},"jack");

throws_ok {
    $b->index([{ author => 'jack', group_by => "23" }],$settings);
} qr/Expected map/;
$settings->{settings}->{expect_array} = true();
$settings->{mapping}->{author}->{type} = "int";
throws_ok {
    $b->index([{ author => 'jack', group_by => "23" }],$settings);
} qr/Expected integer/;

$settings->{mapping}->{author}->{type} = "int";
lives_ok {
    $b->index([{ author => 5, group_by => "23" },{ author => 5, group_by => "24" }],$settings);
} qr/Unexpected raw value/;

$b->delete();
throws_ok {
    $b->search({ term => { author => "jack" } },{ items_per_group => 2, size => 1});
} qr/index is not loaded yet/;

done_testing();
