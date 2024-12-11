package App::BackupZ;

use strict;
use warnings;
use experimental qw(signatures);

use Capture::Tiny qw(capture);
use Feature::Compat::Try;
use JSON qw(to_json from_json);

my %verb_handlers = (
    sync        => \&sync,
    snapshot    => \&snapshot,
    help        => \&help,
    sample_conf => \&sample_conf,
    list        => \&list,
);
my $config;
my $verbose = 0;
my $have_logged;

sub run ($verb, @args) {
    my $words = [];
    my $args = {};

    while(@args) {
        my $arg = shift(@args);

        if($arg =~ /^-/) {
            if($arg eq '-c') {
                my $file = shift(@args);
                if(-r $file && !-d $file) { _load_config($file); }
                 else { err_help("Configuration file not found: $file"); }
            } elsif($arg eq '-v') {
                $verbose++;
            } else {
                err_help("Unknown option: $arg");
            }
        } else {
            push @$words, $arg;
        }
    }

    if(exists($verb_handlers{$verb})) {
        $verb_handlers{$verb}->($args, $words);
    } else {
        err_help("Unknown verb: $verb");
    }
    _exit(0);
}

sub _load_config ($file) {
    try {
        open(my $fh, '<', $file) || die "Can't open $file: $!";
        local $/;
        my $json = <$fh>;
        $config = from_json($json, { relaxed => 1 });
    } catch ($e) {
        err_help("Error loading configuration file: $e");
    }

    $config->{dataset_mountpoint} = _dataset_to_mountpoint($config->{dataset});

    if($config->{logfile}) {
        open(my $logfh, '>>', $config->{logfile}) || err_help("Can't open log file: $config->{logfile}: $!");
        $config->{logfh} = $logfh;
    } else {
        err_help("No logfile specified in configuration");
    }
}

sub _dataset_to_mountpoint ($dataset) {
    my($mountpoint, $stderr, $exit) = capture {
        system('zfs', 'get', '-H', '-o', 'value', 'mountpoint', $dataset);
    };
    if($exit) {
        err_help("Error getting mountpoint for dataset: $stderr");
    }
    chomp($mountpoint);

    return $mountpoint;
}

sub sync ($args, $words) {
    my $sources = $config->{sources};

    foreach my $word (@{$words}) {
        if(!exists($sources->{$word})) {
            err_help("Unknown source: $word");
        }
    }

    foreach my $source (keys %{$sources}) {
        next if(@{$words} && !grep { $_ eq $source } @{$words});
        my $source_details = $sources->{$source};

        my $syncer = $config->{syncers}->{$source_details->{type}};

        if(!$syncer) {
            err_help("Unknown syncer type: $source: $source_details->{type}");
        }

        my $command_parts = $syncer->{command};

        my $source = $source_details->{source};
        my $destination = $source_details->{destination};
        my $options = [
            @{$syncer->{options} || []},
            @{$source_details->{extra_options} || []}
        ];

        my @command = map {
            my $part = $_;

            $part eq '$binary'      ? $syncer->{binary}                            :
            $part eq '@options'     ? @$options                                    :
            $part eq '$source'      ? $source                                      :
            $part eq '$destination' ? "$config->{dataset_mountpoint}/$destination" :
            err_help("Unknown command part: $part");
        } @{$command_parts};

        print_log("Syncing $source to $destination");

        my ($stdout, $stderr, $exit) = _execute(@command);

        if($exit) {
            ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
            print_log(
                "Error syncing $source to $destination",
                "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
                "  Exit code: $exit",
                "  STDOUT:", $stdout,
                "  STDERR:", $stderr,
            );
        }
    }
}

sub snapshot ($args, $words) {
    my $retentions = $config->{retentions};
    my $name = shift(@{$words}) || err_help("Snapshot name not specified");

    if(!exists($retentions->{$name})) {
        err("Unknown retention level: $name");
    }

    my $keep = $retentions->{$name}->{keep};

    my @existing_snapshots = _zfs_get_snapshot_names($name);
    if(@existing_snapshots == $keep) {
        print_log("Got $keep '$name' snapshots, pruning $existing_snapshots[0]");
        _zfs_destroy_snapshot($existing_snapshots[0]);
    }

    my $new_snapshot_name = "$config->{dataset}\@$name:"._epoch_to_iso8601(time());
    my @command = (qw(zfs snapshot), $new_snapshot_name);
    print_log("Creating snapshot: $new_snapshot_name");
    my ($stdout, $stderr, $exit) = _execute(@command);

    if($exit) {
        ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
        print_log(
            "Error creating snapshot: $name",
            "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
            "  Exit code: $exit",
            "  STDOUT:", $stdout,
            "  STDERR:", $stderr,
        );
    }
}

sub _zfs_rename_snapshot ($from, $to) {
    my @command = (qw(zfs rename), "$config->{dataset}\@$from", "$config->{dataset}\@$to");
    my ($stdout, $stderr, $exit) = _execute(@command);

    if($exit) {
        ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
        print_log(
            "Error renaming snapshot: $from to $to",
            "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
            "  Exit code: $exit",
            "  STDOUT:", $stdout,
            "  STDERR:", $stderr,
        );
        _exit(1);
    }
}

sub _zfs_destroy_snapshot ($snapshot) {
    my @command = (qw(zfs destroy), "$config->{dataset}\@$snapshot");
    my ($stdout, $stderr, $exit) = _execute(@command);

    if($exit) {
        ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
        print_log(
            "Error destroying snapshot: $snapshot",
            "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
            "  Exit code: $exit",
            "  STDOUT:", $stdout,
            "  STDERR:", $stderr,
        );
        _exit(1);
    }
}

sub _zfs_get_snapshot_names ($prefix = '') {
    my @command = qw(zfs list -H -t snapshot -o name);
    my ($stdout, $stderr, $exit) = _execute(@command);

    if($exit) {
        ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
        print_log(
            "Error getting snapshot names",
            "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
            "  Exit code: $exit",
            "  STDOUT:", $stdout,
            "  STDERR:", $stderr,
        );
        _exit(1);
    }

    my @all_snapshots = map {
        s/^$config->{dataset}\@//r
    } grep {
        /^$config->{dataset}\@$prefix:/
    } split(/\n/, $stdout);

    return sort @all_snapshots;
}

sub list ($args, $words) {
    my @command = do {
        no warnings 'qw';
        qw(zfs list -H -p -t all -o name,used,avail,refer,creation)
    };
    my ($stdout, $stderr, $exit) = _execute(@command);

    if($exit) {
        ($stdout, $stderr) = map { s/^/    /mgr } ($stdout, $stderr);
        print_log(
            "Error listing snapshots",
            "  Command: [".join(', ', map { "'".(s/'/\\'/gr)."'" } @command)."]",
            "  Exit code: $exit",
            "  STDOUT:", $stdout,
            "  STDERR:", $stderr,
        );
        _exit(1);
    }

    my @snapshots = map {
        my $t = [split(/\t/, $_)];
        $t->[0] =~ s/.*@//;
        $t
    } grep {
        /^$config->{dataset}\@/
    } split(/\n/, $stdout);
    my $sync = (map { [
        split(/\t/, $_)
    ] } grep {
        /^$config->{dataset}/ &&
        $_ !~ /^$config->{dataset}\@/
    } split(/\n/, $stdout))[0];

    print "Sync:\n";
    print "  "._dataset_to_mountpoint($sync->[0]).":\n";
    print "    ".
          "Used: "._human_readable($sync->[1]).
          "Avail: "._human_readable($sync->[2])."\n";
    print "\nManaged snapshots:\n";
    foreach my $snapshot (
        sort {
            $b->[4] <=> $a->[4]
        } grep {
            exists($config->{retentions}->{$_->[0] =~ s/:.*//r})
        } @snapshots
    ) {
        print "  $snapshot->[0]:\n";
        print "    ".
              "Used: "._human_readable($snapshot->[1]).
              "Refer: "._human_readable($snapshot->[3])."\n";
    }
    print "\nUnmanaged snapshots:\n";
    foreach my $snapshot (
        sort {
            $b->[4] <=> $a->[4]
        } grep {
            !exists($config->{retentions}->{$_->[0] =~ s/:.*//r})
        } @snapshots
    ) {
        print "  ".($snapshot->[0] =~ s/.*@//r).":\n";
        print "    ".
              "Used: "._human_readable($snapshot->[1]).
              "Refer: "._human_readable($snapshot->[3])."\n";
    }
}

sub _human_readable ($bytes) {
    my $result;
    if($bytes < 2**10) {
        $result = "$bytes B";
    } elsif($bytes < 2**20) {
        $result = sprintf("%.2f KiB", $bytes/1024);
    } elsif($bytes < 2**30) {
        $result = sprintf("%.2f MiB", $bytes/(1024*1024));
    } elsif($bytes < 2**40) {
        $result = sprintf("%.2f GiB", $bytes/(1024*1024*1024));
    } elsif($bytes < 2**50) {
        $result = sprintf("%.2f TiB", $bytes/(1024*1024*1024*1024));
    } else {
        $result = sprintf("%.2f PiB", $bytes/(1024*1024*1024*1024*1024));
    }
    return sprintf("%-14s ", $result);
}

sub _epoch_to_iso8601 ($epoch) {
    my @time_bits = localtime($epoch);
    $time_bits[5] += 1900;
    $time_bits[4]++;
    return sprintf("%d-%02d-%02dT%02d:%02d:%02d", @time_bits[5, 4, 3, 2, 1, 0]);
}

sub _exit ($status) {
    if($have_logged) {
        my $logfh = $config->{logfh};
        print $logfh "\n";
    }
    exit($status);
}

sub _execute (@command) {
    my ($stdout, $stderr, $exit) = capture {
        my $status = system(@command);
        $status >>= 8 unless($status == -1);
        $status;
    };
    chomp($stdout, $stderr);

    return ($stdout, $stderr, $exit);
}

sub print_log (@lines) {
    my $logfh = $config->{logfh};
    foreach my $line (grep { /\S/ } @lines) {
        print $logfh sprintf("%s: %s\n", _epoch_to_iso8601(time()), $line);
        print "$line\n" if($verbose);
        $have_logged = 1;
    }
}

sub err {
    my $msg = shift;
    print STDERR "$msg\n";
    _exit(1);
}

sub err_help {
    my $msg = shift;

    select STDERR;
    print "$msg\n\n" if($msg);
    print _help();

    _exit(1);
}

sub help {
    my $msg = shift;

    print "$msg\n\n" if($msg);
    print _help();

    _exit(0);
}

sub sample_conf {
    print to_json({
        dataset => 'backupzpool',
        logfile => './backupz.log',
        syncers => {
            rsync => {
                command => [
                    '$binary',
                    '@options',
                    '$source',
                    '$destination',
                ],
                binary  => '/usr/local/bin/rsync',
                options => [
                    '-aSH', '-essh', '--delete', '--delete-excluded',
                    '--numeric-ids',
                    '--timeout=300',
                ]
            },
        },
        sources => {
            source1 => {
                type        => 'rsync',
                source      => 'root@machine:/path/to/source',
                destination => 'destination-dir',
            },
            source2 => {
                type          => 'rsync',
                source        => 'root@machine:/path/to/other_source',
                destination   => 'other_destination-dir',
                extra_options => [
                    '--exclude=somefile',
                ],
            },
        },
        retentions => {
            daily   => { keep => 7, },
            weekly  => { keep => 5, },
            monthly => { keep => 12, },
            yearly  => {}
        },
    }, { canonical => 1, pretty => 1 });
}

sub _help {
'
Usage: backupz [verb] [options]

Verbs:
    sync          Pull data from all the configured sources
    snapshot      Create a snapshot of the backup
    help          Display this help
    sample_conf   Show a sample configuration file

Global options:

    -c <file>   Use the specified configuration file
    -v          Be verbose (repeat for more verbosity)

Verb options:

    sync:

        <source name>   Optional, sync only the specified source.
                        Repeat for multiple sources.

    snapshot:

        <name>   Mandatory, the name of the retention level to create.
'
}

1;
