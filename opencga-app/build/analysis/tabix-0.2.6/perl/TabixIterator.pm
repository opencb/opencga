package TabixIterator;

use strict;
use warnings;
use Carp qw/croak/;

require Exporter;

our @ISA = qw/Exporter/;
our @EXPORT = qw/tabix_iter_free/;

our $VERSION = '0.2.0';

require XSLoader;
XSLoader::load('Tabix', $VERSION);

sub new {
  my $invocant = shift;
  my $class = ref($invocant) || $invocant;
  my $self = {};
  bless($self, $class);
  return $self;
}

sub set {
  my ($self, $iter) = @_;
  $self->{_} = $iter;
}

sub get {
  my $self = shift;
  return $self->{_};
}

sub DESTROY {
  my $self = shift;
  tabix_iter_free($self->{_}) if ($self->{_});
}

1;
__END__
