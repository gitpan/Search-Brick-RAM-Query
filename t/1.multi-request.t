use strict;
use warnings;

use Test::More;
use Test::Exception;
use Search::Brick::RAM::Query;
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

for my $n((1,2,8,64)) {
    my @requests = ('127.0.0.1:9000') x $n;
    my @result = query(host     => \@requests,
                       request  => { term => { title => "book" } },
                       settings => { items_per_group => 1, size => 1},
                       brick    => 'RAM',
                       index    => 'default',
                       timeout  => 10);

    is scalar(@result),$n;
    cmp_ok($result[0],'>',0);

    for (@result) {
        is(scalar(@{ $_->{hits} }),scalar(@{ $result[0]->{hits} }));
    }
}
done_testing();
