use strict;
use warnings;

use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query;
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}
my $b = Search::Brick::RAM::Query->new(index => '__test__',host => 'google.com:80');
throws_ok { $b->delete(1) } qr/timeout/;
done_testing();