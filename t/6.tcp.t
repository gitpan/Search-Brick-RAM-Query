use strict;
use warnings;
use Test::More;
use Test::Exception;
use Data::Dumper;
use Search::Brick::RAM::Query qw(tcp);
use Socket qw(PF_INET SOCK_STREAM pack_sockaddr_in inet_ntoa $CRLF);

unless ($ENV{TEST_LIVE}) {
    plan skip_all => "Enable live testing by setting env: TEST_LIVE=1";
}

# most is borrowed from https://github.com/gugod/Hijk/blob/master/lib/Hijk.pm
sub build_http_message {
    my $args = $_[0];
    my $path_and_qs = ($args->{path} || "/") . ( defined($args->{query_string}) ? ("?".$args->{query_string}) : "" );
    return join(
        $CRLF,
        ($args->{method} || "GET")." $path_and_qs " . ($args->{protocol} || "HTTP/1.1"),
        "Host: $args->{host}",
        $args->{body} ? ("Content-Length: " . length($args->{body})) : (),
        $args->{head} ? (
            map {
                $args->{head}[2*$_] . ": " . $args->{head}[2*$_+1]
            } 0..$#{$args->{head}}/2
        ) : (),
        "",
        $args->{body} ? $args->{body} : ()
    ) . $CRLF;
}

sub http {
    my ($host, $args,$timeout) = @_;
    my @results = ();
    tcp($host,build_http_message($args), $timeout,sub {
        my ($r,$input,$nbytes) = @_;
        my $current;
        if (!($current = $r->{__STATE__})) {
            $current->{body}       = "";
            $current->{head}       = "";
            $current->{header}     = {};
            $current->{status_code} = 0;
            $r->{__STATE__} = $current;
        }

        if ($current->{decapitated}) {
            $current->{body} .= $input;
            $current->{block_size} -= $nbytes;
        } else {
            $current->{head} .= $input;
            my $neck_pos = index($current->{head}, "${CRLF}${CRLF}");
            if ($neck_pos > 0) {
                $current->{decapitated} = 1;
                $current->{body} = substr($current->{head}, $neck_pos+4);
                $current->{head} = substr($current->{head}, 0, $neck_pos);
                $current->{proto} = substr($current->{head}, 0, 8);
                $current->{status_code} = substr($current->{head}, 9, 3);
                substr($current->{head}, 0, index($current->{head}, $CRLF) + 2, ""); # 2 = length($CRLF)

                for (split /${CRLF}/o, $current->{head}) {
                    my ($key, $value) = split /: /, $_, 2;
                    $current->{header}->{$key} = $value;
                }

                if ($current->{header}->{'Content-Length'}) {
                    $current->{block_size} = $current->{header}->{'Content-Length'} - length($current->{body});
                } else {
                    $current->{block_size} = 0;
                }
            }
        }

        if ($current->{decapitated} && $current->{block_size} <= 0) {
            push @results, $current;
            return 1;
        }
        return 0;
    });

    return @results;
}

my @r = http(['google.com:80','google.com:80'],{ host => 'google.com' },10);
is (scalar(@r),2);
for my $result(@r) {
    is($result->{status_code},301);
    is($result->{proto},'HTTP/1.1');
    like($result->{body},qr{HREF="http://www.google.com/">here</A>});
    is($result->{header}->{Location},'http://www.google.com/');
}
is ($r[0]->{status_code},$r[1]->{status_code});
is ($r[0]->{body},$r[1]->{body});

throws_ok { http('google.com:80',{ host => 'google.com' },0.01) } qr/timeout/;

done_testing();
