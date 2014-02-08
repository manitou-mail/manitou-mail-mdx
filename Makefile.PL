# The overriden MakeMaker routines are inherited from spamassassin
require 5.8.0;

use ExtUtils::MakeMaker;
use DBI;
use DBD::Pg;

my %opts = (
   AUTHOR => 'Daniel Verite (daniel@manitou-mail.org)',
   ABSTRACT => 'Perl mail-database exchanger for the Manitou-Mail software',
   NAME => 'manitou-mdx',
   VERSION => '1.3.1',
   EXE_FILES => [ qw(script/manitou-mdx script/manitou-mgr script/manitou-spool) ],
   PREREQ_PM => { 'Getopt::Long' => 2.00,
		  'IO' => 0,
		  'IO::Uncompress::Gunzip' => 2.012,
		  'DBI' => 1.32,
		  'DBD::Pg' => 1.30,
		  'HTML::TreeBuilder' => 3.23,
		  'MIME::Entity'=> 5.0,
		  'MIME::Words'=> 5.0,
		  'MIME::Parser'=> 5.0,
		  'MIME::Head'=> 5.0,
		  'MIME::Body'=> 5.0,
		  'Mail::Address'=> 1.0,
		  'Bit::Vector' => 6.4,
		  'Sys::Syslog' => 0.13
		}
);

WriteMakefile(%opts);

package MY;

sub MY::install {
  my $self = shift;
  my @code = split(/\n/, $self->SUPER::install(@_));
  foreach (@code) {
    # Add our install targets as a dependency to all top-level install targets
    s/^(install(?:_[a-z]+)?\s*::?\s*.*)$/$1 conf__install/;
  }
  return join("\n", @code);
}

sub MY::postamble {
  # Install our minimal configuration file
  return<<'EOF';
CONFDIR=$(PREFIX)/share/manitou-mdx
conf__install:
	-$(MKPATH) $(DESTDIR)$(CONFDIR)
	$(CP) etc/manitou-mdx.conf $(DESTDIR)$(CONFDIR)/manitou-mdx.conf.sample
EOF
}

