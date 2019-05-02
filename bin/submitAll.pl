#!/usr/bin/perl -w

use strict;
use Cwd;
use File::Basename;
my $QSUB = dirname($0).'/qsub';
$ENV{TERMCAP}='';

#PATH correction
my %PATH = map {$_ => 1} split(/:/, $ENV{PATH});
$ENV{PATH} = $ENV{PATH} . ":/bin" unless exists $PATH{"/bin"};

my $default_walltime = "01:00:00";

my $ARGS = join ' ', ' ', @ARGV;
@ARGV = ();

if ($ARGS !~ /walltime=/)
{
	$ARGS .= " -l walltime=$default_walltime";
}
my $JOBQUIET = 0;
if ($ARGS =~ / -Q/)
{
	$JOBQUIET = 1;
	$ARGS =~ s/ -Q//;
}

my $USERNAME = $ENV{USER};
my $HOSTNAME = $ENV{HOSTNAME} || `hostname`;
chomp $HOSTNAME;

#parse the job name
my $JOBNAME = '';
$ARGS =~ s/-N\s+(\S+)//;
$JOBNAME = $1;
if (! defined $JOBNAME or  ! length $JOBNAME)
{
	$JOBNAME = "submitAll_$USERNAME-$HOSTNAME:$$"
}

#parse any suffix for the job name
my $JOBFIRST_SUFFIX_m1 = 0;
#$ARGS =~ s/-f\s+(\d+)//;
#$JOBFIRST_SUFFIX_m1 = $1;
#if (! defined $JOBFIRST_SUFFIX_m1 or ! length $JOBFIRST_SUFFIX_m1)
#{
#	$JOBFIRST_SUFFIX_m1 = 0;
#}
#else
#{
#	$JOBFIRST_SUFFIX_m1 = 1;
#}

my $PWD = getcwd;
$PWD =~ s/^\/Users/\/users/;
my $FIRST_JOBCOUNT = $ENV{STARTINDEX} || 0;
my $JOBCOUNT = 0;

my $JOB = "#!/bin/bash\n";

my $lastCmd = undef;
while(<>)
{
	chomp;
	next if $_ =~ /^\s*#/;
	next unless length $_;
	my $job_command = $_;
	$lastCmd = process_command($job_command);
	if ($JOBCOUNT!=$FIRST_JOBCOUNT)
	{
		$JOB .= 'el';	
	}
	$JOB .= 'if [ "$PBS_ARRAYID" == "'.$JOBCOUNT.'" ]; then'."\n";
	$JOB .= process_command($job_command) ."\n";
	
	$JOBCOUNT++;
}
my $qsubCmd;
if ($JOBCOUNT == $FIRST_JOBCOUNT)
{
	warn "WARN No jobs found on STDIN to submit\n";
}
elsif ($JOBCOUNT == 1)
{
	#warn $lastCmd;
	$qsubCmd = "|$QSUB -N \"$JOBNAME\" -V $ARGS -S /bin/bash";
	open (QSUB, $qsubCmd);
	print QSUB "$lastCmd";
	close QSUB;
}
else
{
	$JOBCOUNT--;
	$JOB .= "fi\n";
	
	#warn $JOB;
	$qsubCmd = "|$QSUB -N \"$JOBNAME\" -t $FIRST_JOBCOUNT-$JOBCOUNT -V $ARGS -S /bin/bash";
	open (QSUB, $qsubCmd);
	print QSUB "$JOB";
	close QSUB;
}

my $exitcode = $? >> 8;
warn "qsub exited with non-zero code ($exitcode) for '$QSUB -N $JOBNAME-$JOBCOUNT -t $FIRST_JOBCOUNT-$JOBCOUNT -V $ARGS'\n" unless $exitcode == 0;
exit $exitcode;

sub process_command
{
	my $command =shift;
	my $command_esc= $command;
	$command_esc =~ s/\"/\\"/g;
	my $rtr = "cd $PWD;\n";
	if ($JOBQUIET != 0)
	{
		$rtr .= "echo \"$command_esc\"\n";
	}
	$rtr .= $command;
	return $rtr;
}

