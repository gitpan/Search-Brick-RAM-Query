use strict;
use warnings;

use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query qw(false query);
unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

my $settings = {
    mapping => {
        title => {
            type  => "string",
            index =>  Data::MessagePack::true(),
            store =>  Data::MessagePack::true(),
        },
        f_boost => {
            type  => "float",
            index =>  Data::MessagePack::true()
        },
        year => {
            type  => "int",
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
        expected => 0,
        shards => 6,
    }
};

sub _store {
    my ($data,$expected,$name) = @_;
    $name ||= 'default';
    $settings->{settings}->{expected} = $expected;
    query(index => 'default',action => 'store', request => $data, settings => $settings, brick => 'RAM');
}

sub _store_as_file {
    my ($data,$expected,$name) = @_;
    $name ||= 'default_second';
    $settings->{settings}->{expected} = $expected;
    $settings->{settings}->{store} = "/tmp/index/zzz_stored";
    my $packer = Data::MessagePack->new();
    open(my $fh, ">", "/tmp/index/zzz_default_second.msgpack") || die $!;
    syswrite($fh,$packer->pack($settings));
    syswrite($fh,$packer->pack($data));
    close($fh);
    sleep(5);
}

my @list = ();
my $n = 1000;
for (1..$n) {
    my $g = int(rand() * 100) % 20;
    push @list,{
        "title"   => 'book',
        "year"    => 2014,
        "f_boost" => 3.2,
        "group_by" => "$g"
    };
}

throws_ok { _store(\@list,5) } qr/ must be in group blocks/,'must be in groups';
@list = sort { $a->{group_by} <=> $b->{group_by} } @list;

my $d = shift(@list);
my $g = delete($d->{group_by});
unshift(@list,$d);
throws_ok { _store(\@list,scalar(@list)) } qr/document without group_by field/,'no group';


$list[0]->{group_by} = $g;
throws_ok { _store(\@list,5) } qr/got object with <$n> documents, expected: <5>/,'expected 5';
lives_ok { _store(\@list,scalar(@list)) };
lives_ok { _store_as_file(\@list,scalar(@list)) };

my $b1 = Search::Brick::RAM::Query->new(index => 'default_second_alias');
my $b2 = Search::Brick::RAM::Query->new(index => 'zzz_default_second');
$b1->alias({ add => [{ 'default_second_alias' => 'zzz_default_second' }]});
my @r1 = $b1->search({ term => {title => 'book' }});
my @r2 = $b2->search({ term => {title => 'book' }});
my @stats = $b1->stat();
is ($stats[0]->{'alias.default_second_alias'},'zzz_default_second');
is ($r1[0]->{hits}->[0]->{__score},$r1[0]->{hits}->[1]->{__score});
use Data::Dumper;
my @deleted1 = $b1->delete();
my @deleted2 = $b2->delete();
is ($deleted1[0]->{'default_second_alias'},0,"delete on alias name should fail");
is ($deleted2[0]->{'zzz_default_second'},1,"delete on index name should be ok");
ok(! -f "/tmp/index/zzz_default_second.msgpack","messagepack file still exists");
ok(! -d "/tmp/index/zzz_stored","stored directory still exists");
done_testing();
