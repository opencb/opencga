use ExtUtils::MakeMaker;
WriteMakefile(
			  NAME         => 'Tabix',
			  VERSION_FROM => 'Tabix.pm',
			  LIBS         => ['-lz -L.. -ltabix'],
			  DEFINE       => '-D_FILE_OFFSET_BITS=64 -D_USE_KNETFILE',
			  INC          => '-I..',
			 );
