use ExtUtils::MakeMaker;
use ExtUtils::Manifest qw(mkmanifest);
# See lib/ExtUtils/MakeMaker.pm for details of how to influence
# the contents of the Makefile that is written.
mkmanifest();
WriteMakefile(
    NAME              => 'Search::Brick::RAM::Query',
    VERSION_FROM      => 'lib/Search/Brick/RAM/Query.pm', # finds $VERSION
    LICENSE           => 'perl_5',
    PREREQ_PM         => {
        Data::MessagePack => 0.48,
        Data::MessagePack::Stream => 0.07,
        Test::Exception => 0.32,
        IO::Select => 1.17,
        Time::HiRes => 1.9725,
        Data::Dumper => 2.145
    },
    ($] >= 5.005 ?     ## Add these new keywords supported since 5.005
      (ABSTRACT_FROM  => 'lib/Search/Brick/RAM/Query.pm', # retrieve abstract from module
       AUTHOR         => 'Borislav Nikolov <jack@sofialondonmoskva.com>') : ()),
);
