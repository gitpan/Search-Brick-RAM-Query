use ExtUtils::MakeMaker;
use ExtUtils::Manifest qw(mkmanifest);
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
mkmanifest();
WriteMakefile(
    NAME              => 'Search::Brick::RAM::Query',
    VERSION_FROM      => 'lib/Search/Brick/RAM/Query.pm', # finds $VERSION
    PREREQ_PM         => {}, # e.g., Module::Name => 1.1
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Search/Brick/RAM/Query.pm', # retrieve abstract from module
       AUTHOR         => 'Borislav Nikolov <jack@sofialondonmoskva.com>') : ()),
);
