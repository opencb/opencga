#-*-Perl-*-
use Test::More tests => 9;
BEGIN { use_ok('Tabix') };

{ # C-like low-level interface
	my $t = tabix_open("../example.gtf.gz");
	ok($t);
	my $iter = tabix_query($t, "chr1", 0, 2000);
	ok($iter);
	$_ = 0;
	++$_ while (tabix_read($t, $iter));
	is($_, 6);
	tabix_iter_free($iter);
	@_ = tabix_getnames($t);
	is(scalar(@_), 2);
}

{ # OOP high-level interface
	my $t = Tabix->new(-data=>"../example.gtf.gz");
	ok($t);
	my $iter = $t->query("chr1", 3000, 5000);
	ok($iter);
	$_ = 0;
	++$_ while ($t->read($iter));
	is($_, 27);
	@_ = $t->getnames;
	is($_[1], "chr2");
}
