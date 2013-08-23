#-*-Perl-*-
use Test::More tests => 9;
BEGIN { use_ok('Tabix') };

{ # FTP access
	my $t = Tabix->new(-data=>"ftp://ftp.ncbi.nih.gov/1000genomes/ftp/pilot_data/release/2010_03/pilot1/CEU.SRP000031.2010_03.genotypes.vcf.gz");
	ok($t);
	my $iter = $t->query("1", 1000000, 1100000);
	ok($iter);
	$_ = 0;
	++$_ while ($t->read($iter));
	is($_, 306);
	@_ = $t->getnames;
	is(scalar(@_), 22);
}

{ # FTP access plus FTP index
	my $t = Tabix->new(-data=>"ftp://ftp.ncbi.nih.gov/1000genomes/ftp/pilot_data/release/2010_03/pilot1/CEU.SRP000031.2010_03.genotypes.vcf.gz",
					   -index=>"ftp://ftp.ncbi.nih.gov/1000genomes/ftp/pilot_data/release/2010_03/pilot1/CEU.SRP000031.2010_03.genotypes.vcf.gz.tbi");
	ok($t);
	my $iter = $t->query("19", 10000000, 10100000);
	ok($iter);
	$_ = 0;
	++$_ while ($t->read($iter));
	is($_, 268);
	@_ = $t->getnames;
	is(scalar(@_), 22);
}
