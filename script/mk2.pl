#!/usr/bin/perl
use strict;
use warnings;
our $VERSION = 'v1.0.0';

package Object;
use Carp qw/ cluck confess /;
use Moo;
use Data::Dumper;
use POSIX;
ACCESSORS: {
	has tablestack => (
		is      => 'rw',
		lazy    => 1,
		default => sub { $_[0]->gettablestack() || [] }
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
	has dbh => (
		is      => 'rw',
		lazy    => 1,
		default => sub {

			#this will intentionally fail on init - to be replaced in inheritor classes
			my $self = shift;
			return $self->_set_dbh();
		}
	);
}

#dbh overwrites DB's version
with qw/
  Moo::GenericRole::DB
  Moo::GenericRole::FileSystem
  Moo::GenericRole::CombinedCLI
  /;

sub _set_dbh {

	my ( $self, $args ) = @_;
	$args ||= $self->cfg();
	my $driver = $args->{driver} || 'mysql';

	#does weird things on older perl versions
	my $dsn = "dbi:$driver:$args->{db};host=$args->{host};";
	$dsn .= $args->{dsn_extra} if $args->{dsn_extra};
	my $dbh = DBI->connect( $dsn, $args->{user}, $args->{pass}, $args->{dbattr} || {} ) or die $!;
	return $dbh;

}

sub gettablestack {

	my ( $self ) = @_;
	my $suffix   = $self->cfg->{show_suffix} || '';
	my $sth      = $self->dbh->prepare( "show tables $suffix" );
	$sth->execute();
	my $return;
	while ( my $row = $sth->fetchrow_arrayref() ) {

		# 		warn $row->[0];
		push( @{$return}, $row->[0] );
	}
	return $return; # return!

}

sub criticalpath {

	my ( $self ) = @_;
	for my $table ( @{$self->tablestack()} ) {
		warn $table;
		$self->processtable( $table );
	}
	if ( $self->gitmode ) {
		my $cstring = "git -C " . $self->cfg->{path} . " add " . $self->cfg->{path} . '/*';
		`$cstring`;
		$cstring = "git -C " . $self->cfg->{path} . " commit -am 'autocommit at" . POSIX::strftime( "%Y:%m:%dT%H:%M:%S", gmtime() ) . "'";
		`$cstring`;
		$cstring = "git push";
		`$cstring`;
	}

}

sub processtable {

	my ( $self, $table, $c ) = @_;
	my $dir ||= $self->abs_path( $self->cfg->{path} . "/$table/" );
	$self->mk_path( $dir ) unless -e $dir;
	warn $dir;
	unless ( $self->cfg->{skip_structure} ) {
		$self->process_structure_dump( $table, $c, $dir );
	}
	unless ( $self->cfg->{skip_data} ) {
		$self->process_data_dump( $table, $c, $dir );
	}

}

sub process_structure_dump {

	my ( $self, $table, $c, $dir ) = @_;
	$dir ||= $self->abs_path( $self->cfg->{path} . "/$table/" );
	$self->mk_path( $dir ) unless -e $dir;
	$self->handledump(
		{
			dir        => $dir,
			table      => $table,
			type       => 'structure',
			dumpparams => '--no-data',
		}
	);

}

sub process_data_dump {

	my ( $self, $table, $c, $dir ) = @_;
	$dir ||= $self->abs_path( $self->cfg->{path} . "/$table/" );
	$self->mk_path( $dir ) unless -e $dir;
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
	my $args       = $self->cfg();
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
	warn $cstring;
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
	require Digest::MD5;
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

	my $obj = Object->new();
	$obj->get_config(
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
			  skip_data
			  skip_structure
			  show_suffix
			  optstring
			  dsn_extra
			  /
		]
	);
	$obj->criticalpath();

}
