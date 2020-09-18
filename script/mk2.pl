#!/usr/bin/perl
use strict;
use warnings;

package Object;
use Carp qw/ cluck confess /;
use Moo;
use Data::Dumper;
use POSIX;
with qw/
  Moo::Role::DB
  Moo::Role::FileSystem
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
		default => sub { return {} }
	);
	has gitmode => (
		is      => 'rw',
		lazy    => 1,
		default => sub { return undef }
	);
	has skipdelta => (
		is      => 'rw',
		lazy    => 1,
		default => sub { return undef }
	);
}

sub BUILD {
	my ( $self, $args ) = @_;
	my $driver = $args->{driver} || 'mysql';
	my $dbh = DBI->connect( "dbi:$driver:$args->{db};host=$args->{host}", $args->{user}, $args->{pass}, $args->{dbattr} || {} ) or die $!;
	$self->dbh( $dbh );

	$self->args( $args );
}

sub gettablestack {
	my ( $self ) = @_;

	my $sth = $self->dbh->prepare( "show tables" );
	$sth->execute();
	my $return;

	while ( my $row = $sth->fetchrow_arrayref() ) {

		push( @{$return}, $row->[0] );
	}
	return $return; # return!
}

sub criticalpath {
	my ( $self ) = @_;
	for my $table ( @{$self->tablestack()} ) {
		$self->processtable( $table );
	}
	if ( $self->gitmode ) {
		my $cstring = "git -C " . $self->path() . " add " . $self->path() . '/*';
		`$cstring`;
		$cstring = "git -C " . $self->path() . " commit -am 'autocommit at" . POSIX::strftime( "%Y:%m:%dT%H:%M:%S", gmtime() ) . "'";
		`$cstring`;
		$cstring = "git push";
		`$cstring`;
	}
}

sub processtable {
	my ( $self, $table, $c ) = @_;

	my $dir = $self->abspath( $self->path() . "/$table/" );
	$self->mkpath( $dir );

	$self->handledump(
		{
			dir        => $dir,
			table      => $table,
			type       => 'structure',
			dumpparams => '--no-data',
		}
	);
	$self->handledump(
		{
			dir        => $dir,
			table      => $table,
			type       => 'data',
			dumpparams => '--no-create-info',
		}
	);
}

sub handledump {
	my ( $self, $c ) = @_;
	my $args       = $self->args();
	my $mysqldump  = $self->mysqldump;
	my $timestring = POSIX::strftime( "%Y:%m:%dT%H:%M:%S", gmtime() );
	my $exportname;
	if ( $self->skipdelta ) {
		$exportname = "$c->{table}\_$c->{type}.sql";
	} else {
		$exportname = "$c->{table}\_$c->{type}_$timestring.sql";
	}

	my $exportpath = "$c->{dir}/$exportname";
	my $pstring    = '';
	if ( $args->{pass} ) {
		$pstring = "-p$args->{pass}";
	}

	#makes the exports actually readable without crashing kwrite
	my $optstring = '--net_buffer_length=4096';
	if ( $args->{optstring} ) {
		$optstring = $args->{optstring};
	}

	my $cstring = "$mysqldump $c->{dumpparams} --skip-comments --skip-add-locks -h $args->{host} -u $args->{user} $pstring $optstring $args->{db} $c->{table} > $exportpath";

	#warn $cstring;
	`$cstring`;

	#if skipdelta is set it just overwrote the existing file and we don't care what happens
	unless ( $self->skipdelta ) {

		my $fixed = "$c->{dir}/$c->{table}\_$c->{type}.sql";
		if ( -e $fixed ) {
			my $old     = $self->digestfile( $fixed );
			my $current = $self->digestfile( $exportpath );
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
}

sub digestfile {
	my ( $self, $path ) = @_;
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

			  /
		],
		[
			qw/
			  driver
			  pass
			  mysqldump
			  dbattr
			  gitmode
			  skipdelta
			  /
		]
	);
	my $obj = Object->new( $clv );
	$obj->criticalpath();
}

