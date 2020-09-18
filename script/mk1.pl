#!/usr/bin/perl
use strict;
use warnings;

package Object;
use Carp qw/ cluck confess /;
use Moo;
use Data::Dumper;
use POSIX;
with qw/
  Moo::GenericRole::DB
  Moo::GenericRole::FileSystem
  /;
ACCESSORS: {
	has tablestack => (
		is      => 'rw',
		lazy    => 1,
		default => sub { $_[0]->gettablestack() }
	);
	has path => (
		is   => 'rw',
		lazy => 1,
	);
	has mysqldump => (
		is      => 'rw',
		lazy    => 1,
		default => sub { 'mysqldump' }
	);
	has args => (
		is      => 'rw',
		lazy    => 1,
		default => sub { 'mysqldump' }
	);
}

sub BUILD {

	my ( $obj, $args ) = @_;
	my $dbh = DBI->connect( "dbi:$args->{driver}:$args->{db};host=$args->{host}", $args->{user}, $args->{pass}, $args->{dbattr} || {} ) or die $!;
	$obj->dbh( $dbh );
	$obj->args( $args );

}

sub gettablestack {

	my ( $obj ) = @_;
	my $sth = $obj->dbh->prepare( "show tables" );
	$sth->execute();
	my $return;
	while ( my $row = $sth->fetchrow_arrayref() ) {
		push( @{$return}, $row->[0] );
	}
	return $return; # return!

}

sub criticalpath {

	my ( $obj ) = @_;
	for my $table ( @{$obj->tablestack()} ) {
		$obj->processtable( $table );
	}

}

sub processtable {

	my ( $obj, $table, $c ) = @_;
	my $dir = $obj->abspath( $obj->path() . "/$table/" );
	$obj->mkpath( $dir );
	$obj->handledump(
		{
			dir        => $dir,
			table      => $table,
			type       => 'structure',
			dumpparams => '--no-data',
		}
	);
	$obj->handledump(
		{
			dir        => $dir,
			table      => $table,
			type       => 'data',
			dumpparams => '--no-create-info',
		}
	);

}

sub handledump {

	my ( $obj, $c ) = @_;
	my $args       = $obj->args();
	my $mysqldump  = $obj->mysqldump;
	my $timestring = POSIX::strftime( "%Y:%m:%dT%H:%M:%S", gmtime() );
	my $exportname = "$c->{table}\_$c->{type}_$timestring.sql";
	my $exportpath = "$c->{dir}/$exportname";
	my $pstring    = '';
	if ( $args->{pass} ) {
		$pstring = "-p$args->{pass}";
	}
	my $cstring = "$mysqldump $c->{dumpparams} --skip-comments --skip-add-locks -h $args->{host} -u $args->{user} $pstring $args->{db} $c->{table} > $exportpath";

	# 		warn $cstring;
	`$cstring`;
	my $fixed = "$c->{dir}/$c->{table}\_$c->{type}.sql";
	if ( -e $fixed ) {
		my $old     = $obj->digestfile( $fixed );
		my $current = $obj->digestfile( $exportpath );
		if ( $old eq $current ) {
			unlink $exportpath;
		} else {
			unlink $fixed;
			`cp $exportpath $fixed`;
		}
	} else {
		`cp $exportpath $fixed`;
	}

}

sub digestfile {

	my ( $obj, $path ) = @_;
	open( my $fh, '<:raw', $path )
	  or die "failed to open digest file [$path] : $!";
	my $ctx = Digest::MD5->new;
	$ctx->addfile( $fh );
	close( $fh );
	return $ctx->hexdigest();

}
1;

package main;
use Carp qw/ cluck confess /;
use Toolbox::CombinedCLI;
use DBI;
use Data::Dumper;
main();

sub main {

	my $clv = Toolbox::CombinedCLI::get_config(
		[
			qw/
			  path
			  user
			  db
			  host
			  driver
			  /
		],
		[
			qw/
			  pass
			  mysqldump
			  dbattr
			  data_only
			  structure_only
			  /
		]
	);
	my $obj = Object->new( $clv );
	if ( $clv->{data_only} || $clv->{structure_only} ) {
		if ( $clv->{data_only} ) {
			my $dir = $obj->abspath( $obj->path() . "/$table/" );
			$obj->mkpath( $dir );
			$obj->handledump(
				{
					dir        => $dir,
					table      => $table,
					type       => 'data',
					dumpparams => '--no-create-info',
				}
			);
		} elsif {
			my $dir = $obj->abspath( $obj->path() . "/$table/" );
			$obj->mkpath( $dir );
			$obj->handledump(
				{
					dir        => $dir,
					table      => $table,
					type       => 'structure',
					dumpparams => '--no-data',
				}
			);
		} else {
			$obj->criticalpath();
		}
	}

}
