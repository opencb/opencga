package Tabix;

use strict;
use warnings;
use Carp qw/croak/;

use TabixIterator;

require Exporter;

our @ISA = qw/Exporter/;
our @EXPORT = qw/tabix_open tabix_close tabix_read tabix_query tabix_getnames tabix_iter_free/;

our $VERSION = '0.2.0';

require XSLoader;
XSLoader::load('Tabix', $VERSION);

sub new {
  my $invocant = shift;
  my %args = @_;
  $args{-data} || croak("-data argument required");
  my $class = ref($invocant) || $invocant;
  my $self = {};
  bless($self, $class);
  $self->open($args{-data}, $args{-index});
  return $self;
}

sub open {
  my ($self, $fn, $fnidx) = @_;
  $self->close;
  $self->{_fn} = $fn;
  $self->{_fnidx} = $fnidx;
  $self->{_} = $fnidx? tabix_open($fn, $fnidx) : tabix_open($fn);
}

sub close {
  my $self = shift;
  if ($self->{_}) {
	tabix_close($self->{_});
	delete($self->{_}); delete($self->{_fn}); delete($self->{_fnidx});
  }
}

sub DESTROY {
  my $self = shift;
  $self->close;
}

sub query {
  my $self = shift;
  my $iter;
  if (@_) {
	$iter = tabix_query($self->{_}, @_);
  } else {
	$iter = tabix_query($self->{_});
  }
  my $i = TabixIterator->new;
  $i->set($iter);
  return $i;
}

sub read {
  my $self = shift;
  my $iter = shift;
  return tabix_read($self->{_}, $iter->get);
}

sub getnames {
  my $self = shift;
  return tabix_getnames($self->{_});
}

1;
__END__
