#!/usr/bin/evn perl

use 5.010;
use strict;
use lib::abs '../../../libs/*/lib','../../../libs/*/blib/lib','../../../libs/*/blib/arch';
use EV;
use EV::Tarantool;
use JSON::XS;
use Scalar::Util 'weaken';
use DDP;
our $JSON = JSON::XS->new->utf8;

# use for testing latency:
# lua -tonumber64(jobs.task('any',tostring(box.time64()),1)) + box.time64()

my $tnt = EV::Tarantool->new({
	timeout => 10,
	host => 0,
	port => 33013,
	connected => sub {
		my $c = shift;
		$c->lua('jobs.worker',[],{out => 'p'},sub {
			if (my $res = shift) {
				my $wid = $res->{tuples}[0][0];
				say "Registered as worker id $wid";
				my $timeout = 1;
				my $loop;$loop = sub {
					$c->lua('jobs.work',[ $wid,$timeout ], sub {
						if (my $t = shift) {
							if ($t->{count} > 0) {
								# p $t->{tuples};
								for my $task ( @{ $t->{tuples} } ) {
									my ($tid,@data) = @$task;
									say "got task $tid";
									# ...
									#my $j;$j = EV::timer 1,0,sub { undef $j;
										$c->lua('jobs.done',[$tid,@data], sub {
											shift or warn "@_";
										});
									#};
								}
							}
							$loop->();
						}
						else {
							p @_;
							return;
						}
					});
				};
				$loop->();
				my $w;$w = EV::timer $timeout/2,0, sub { undef $w; $loop && $loop->(); };
				weaken($loop);
			}
			else {
				p @_;
			}
		});
		return;
	},
	disconnected => sub {
		shift;
		warn "@_";
	},
});
$tnt->connect;
EV::loop;