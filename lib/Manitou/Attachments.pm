# Copyright (C) 2004-2010 Daniel Verite

# This file is part of Manitou-Mail (see http://www.manitou-mail.org)

# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 2 as
# published by the Free Software Foundation.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330,
# Boston, MA 02111-1307, USA.

package Manitou::Attachments;

use strict;
use vars qw(@ISA @EXPORT_OK);

use File::stat;
use Carp;
use POSIX qw(tmpnam);
use Encode;
use Manitou::Encoding qw(encode_dbtxt header_decode);
use Digest::SHA1;

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw(flatten_and_insert_attach detach_text_attachments
		attach_parts create_html_part has_attachments);

sub get_sequence_nextval {
  my ($dbh, $seq) = @_;
  my ($nextval, $sth, $row);
  $sth=$dbh->prepare("SELECT nextval('".$seq."')") or die $dbh->errstr;
  $sth->execute() or die $dbh->errstr;
  my @row = $sth->fetchrow_array;
  if ($row[0]) {
    $nextval = $row[0];
  } else {
    $nextval = 1;
  }
  $sth->finish;
  return $nextval;
}

sub detach_text_attachments {
  my ($dbh, $top, $mail_id, $pbody_text, $pbody_html) = @_;
  my $attachments;
  if ($top->is_multipart) {
    # Identify text/plain + text/html combination to insert
    # into body (bodytext,bodyhtml) instead of putting the HTML part
    # into the attachment table.
    if ($top->effective_type eq "multipart/alternative" &&
	$top->parts==2 &&
	$top->parts(0)->bodyhandle &&
	$top->parts(1)->bodyhandle &&
	( ($top->parts(0)->effective_type eq "text/plain" &&
	   $top->parts(1)->effective_type eq "text/html") ||
	  ($top->parts(0)->effective_type eq "text/html" &&
	   $top->parts(1)->effective_type eq "text/plain")
	))
      {
	for my $i (0,1) {
	  my $p = $top->parts($i);
	  my $charset = $p->head->mime_attr("content-type.charset") || 'iso-8859-1';
	  $charset ='iso-8859-1' if (!Encode::resolve_alias($charset));
	  if ($p->effective_type eq "text/plain") {
	    $$pbody_text .= Encode::decode($charset, $p->bodyhandle->as_string);
	  }
	  else {
	    # For HTML, we don't concatenate parts. If contents have already been put
	    # in the HTML part upper in the recursion steps, then the new contents
	    # are inserted into the attachment table.
	    if (!defined $$pbody_html) {
	      $$pbody_html = Encode::decode($charset, $p->bodyhandle->as_string);
	    }
	    else {
	      flatten_and_insert_attach($dbh, $p, $mail_id);
	    }
	  }
	}
      }
    else {
      # Generic case. Concat text/plain parts with no filename to the text body
      # and insert other parts into the attachment table.
      foreach my $p ($top->parts) {
	if ($p->effective_type eq "text/plain" &&
	    $p->head->recommended_filename eq "" &&
	    $p->bodyhandle) {
	  my $charset = $p->head->mime_attr("content-type.charset") || 'iso-8859-1';
	  $charset='iso-8859-1' if (!Encode::resolve_alias($charset));
	  $$pbody_text .= Encode::decode($charset, $p->bodyhandle->as_string);
	}
	else {
	  $attachments += detach_text_attachments($dbh, $p, $mail_id, $pbody_text, $pbody_html);
	}
      }
    }
  }
  else {
    $attachments = flatten_and_insert_attach($dbh, $top, $mail_id);
  }
  return $attachments;
}

sub flatten_and_insert_attach {
  my ($dbh, $top,$mail_id) = @_;
  my $attachments;
  if ($top->is_multipart) {
    foreach ($top->parts) {
      $attachments+=flatten_and_insert_attach($dbh, $_, $mail_id);
    }
  }
  else {
    if ($top->bodyhandle) {
      &insert_attachment($dbh, $mail_id, $top);
      $attachments++;
    }
  }
  return $attachments;
}

# remove undesirable characters
sub sanitize_filename {
  my $f=$_[0];
  $f =~ s/\/\\\"\'\<\>//g;
  $f =~ s/\s/_/g;
  $f =~ tr/\x00-\x1F//d;
  return $f;
}

sub insert_attachment {
  my ($dbh, $mail_id, $mime_obj) = @_;
  my $attachment_id = get_sequence_nextval($dbh, "seq_attachment_id");
  my $lobjId;
  my $attch_file=tmpnam();
  open(PGIN, ">$attch_file") or die "can not open $attch_file: $!";
  $mime_obj->bodyhandle->print(\*PGIN);
  close(PGIN);
  my $filesize = stat($attch_file)->size;

  my $stha = $dbh->prepare("INSERT INTO attachments(attachment_id,mail_id,content_type,content_size,filename,charset,mime_content_id) VALUES (?,?,?,?,?,?,?)")  or die $dbh->errstr;

  my $pos = 0;
  $stha->bind_param(++$pos, $attachment_id);
  $stha->bind_param(++$pos, $mail_id);
  my $a_type=substr(header_decode($mime_obj->effective_type),0,300);
  $stha->bind_param(++$pos, encode_dbtxt($a_type));
  $stha->bind_param(++$pos, $filesize);

  my $fname=header_decode($mime_obj->head->recommended_filename);
  $fname=sanitize_filename(substr($fname,0,300));
  $stha->bind_param(++$pos, encode_dbtxt($fname));

  my $charset=header_decode($mime_obj->head->mime_attr("content-type.charset"));
  $stha->bind_param(++$pos, encode_dbtxt(substr($charset,0,30)));
  LogError($stha->errstr) if $stha->err;

  my $content_id=$mime_obj->get("Content-ID");
  # content-ID syntax must be <addr-spec> (RFC2111)
  $content_id = ($content_id =~ /^\<(.*)\>$/) ? $1 : undef;
  $stha->bind_param(++$pos, encode_dbtxt(header_decode($content_id)));

  $stha->execute or die $stha->errstr;
  $stha->finish;

  if ($filesize>0) {
    # compute the fingerprint
    my $sha1 = Digest::SHA1->new;
    open(PGIN, "$attch_file") or die "can not open $attch_file: $!";
    binmode PGIN;
    $sha1->addfile(*PGIN);
    my $fingerprint = $sha1->b64digest;
    # check if the content already exists in the database
    my $sth1=$dbh->prepare("SELECT content FROM attachment_contents WHERE fingerprint=?");
    $sth1->execute($fingerprint) or die $sth1->errstr;
    ($lobjId)=$sth1->fetchrow_array;
    $sth1->finish;
    if (!$lobjId) {
      # import the contents
      $lobjId = $dbh->func($attch_file, 'lo_import');
    }

    my $sth = $dbh->prepare("INSERT INTO attachment_contents(attachment_id, content, fingerprint) VALUES (?,?,?)") or die $dbh->errstr;
    $sth->bind_param(1, $attachment_id);
    $sth->bind_param(2, $lobjId);
    $sth->bind_param(3, $fingerprint);

    $sth->execute or die $sth->errstr;
    $sth->finish;
  }
  unlink($attch_file);
}

# Create the file if it is a file or put the attachment contents to
# the location pointed to by $content
sub get_one_attachment {
  my ($dbh, $a_id,$filename,$content_size,$content,$tmpdir)=@_;
  my $ret;
  my $sth = $dbh->prepare ("SELECT content FROM attachment_contents WHERE " .
			   "attachment_id=?") ||
			     die "Can't prepare statement: $DBI::errstr";
  $sth->execute($a_id) || die "Can't execute statement: $DBI::errstr";

  my @row = $sth->fetchrow_array;
  die $sth->errstr if $sth->err;
  my $lobjId = $row[0];
  $sth->finish;

  # Note: this needs to run while inside a transaction (this is required
  # for the lo_* functions)
  if ($filename) {
    $ret = $dbh->func ($lobjId, $tmpdir . "/$filename", 'lo_export');
  }
  else {
    $$content = "";
    my $lobj_fd = $dbh->func ($lobjId, $dbh->{pg_INV_READ}, 'lo_open');
    die $dbh->errstr if (!defined($lobj_fd));
    my $buf;
    my $nbytes;
    while ($content_size > 0) {
      $nbytes = $dbh->func($lobj_fd, $buf, 16384, 'lo_read');
      die $dbh->errstr if (!defined($nbytes));
      $content_size -= $nbytes;
      $$content .= $buf;
    }
    $dbh->func ($lobj_fd, 'lo_close');
  }
}

# Returns true if there are files to be attached, not counting the files
# related to the HTML parts
sub has_attachments {
  my ($dbh, $mail_id)=@_;
  my $sth = $dbh->prepare("SELECT count(*) FROM attachments WHERE mail_id=? AND mime_content_id IS NULL");
  $sth->execute($mail_id);
  my @r=$sth->fetchrow_array();
  $sth->finish;
  return ($r[0]>0);
}

# TODO: find a better way to ignore attachments related to the HTML
# part than mime_content_id IS NULL
sub attach_parts {
  my ($dbh,$mail_id,$mobj,$tmpdir)=@_;

  my $sth = $dbh->prepare ("SELECT attachment_id,content_type,content_size,filename,mime_content_id FROM attachments WHERE mail_id=? AND mime_content_id IS NULL") || die "Can't prepare statement: $DBI::errstr";
  $sth->execute($mail_id) || die "Can't execute statement: $DBI::errstr";
  while (my $row = $sth->fetchrow_hashref) {
    my $attch_data;
    $row->{filename} = sanitize_filename($row->{filename}) if defined($row->{filename});
    get_one_attachment($dbh, $row->{attachment_id}, $row->{filename},
		       $row->{content_size}, \$attch_data, $tmpdir);
    my %args = ('Encoding' => '-SUGGEST',
		'Disposition' => 'attachment',
		'Type' => $row->{content_type});
    if (defined $row->{filename}) {
      $args{'Path'} = $tmpdir . "/" . $row->{filename};
    }
    else {
      $args{'Data'} = $attch_data;
    }
#    if ($mime_content_id) {
#      $args{'Id'} = $mime_content_id;
#    }
    $mobj->attach(%args);
  }
  $sth->finish;
}

# Create the MIME part that holds the HTML contents.
# If the HTML body references external objects with CIDs (probably
# images), then we create a multipart/related, otherwise a single part.
# At the moment, an attachment is assumed to be related to the HTML contents
# if and only if it has a mime_content_id. In a future version, the MIME structure
# might be laid out in the database in advance to provide for more flexibility.
sub create_html_part {
  my ($dbh, $mail_id, $ref_html_text) = @_;
  my $sth = $dbh->prepare ("SELECT attachment_id,content_type,content_size,mime_content_id FROM attachments WHERE mail_id=? AND mime_content_id IS NOT NULL ORDER BY 1") || die "Can't prepare statement: $DBI::errstr";
  $sth->execute($mail_id) || die "Can't execute statement: $DBI::errstr";
  my $part;
  if ($sth->rows > 0) {
    $part = MIME::Entity->build('Type' => 'multipart/related');
    $part->attach('Data' => $$ref_html_text,
		  'Encoding' => '-SUGGEST',
		  'Type' => 'text/html',
		  'Charset' => 'utf-8');
    while (my $row = $sth->fetchrow_hashref) {
      my $attch_data;
      get_one_attachment($dbh, $row->{attachment_id}, undef,
			 $row->{content_size}, \$attch_data);
      $part->attach('Encoding' => '-SUGGEST',
		    'Type' => $row->{content_type},
		    'Data' => $attch_data,
		    'Id' => "<" . $row->{mime_content_id} . ">"
		   );
    }
  }
  else {
    $part = MIME::Entity->build('Data' => $$ref_html_text,
				'Encoding' => '-SUGGEST',
			        'Type' => 'text/html',
			        'Charset' => 'utf-8',
			        'X-Mailer' => undef);
  }
  $sth->finish;
  return $part;
}
