# Copyright (C) 2004-2024 Daniel Verite

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

package Manitou::Mailing;

use strict;
use vars qw(@ISA @EXPORT_OK);


use Manitou::MailFormat;
use Manitou::Config qw(getconf);
use Manitou::Database qw(db_connect);
use Manitou::Encoding qw(decode_dbtxt);
use Manitou::Log qw(error_log notice_log);
use IPC::Open3;
use IO::Handle;
use Manitou::xSV;
use File::Temp qw(tempfile);

require Exporter;
@ISA = qw(Exporter);
@EXPORT_OK = qw();

sub merge_fields {
  my ($template_txt, $p_values) = @_;
  my $txt=$template_txt;
  for my $k (keys %{$p_values}) {
    $txt =~ s/\Q{{$k}}\E/$$p_values{$k}/g;
  }
  return $txt;
}

# Newlines in header fields are filtered out since it's
# the separator between headers for MailFormat::encode_header
sub merge_fields_header {
  my ($template_txt, $p_values) = @_;
  my $txt=$template_txt;
  for my $k (keys %{$p_values}) {
    my $v = $$p_values{$k};
    $v =~ s/\r\n/\n/g;
    $v =~ s/\r/\n/g;
    $v =~ s/\n/ /g;
    $txt =~ s/\Q{{$k}}\E/$v/g;
  }
  return $txt;
}

# return 1 if no error
sub deliver_message {
  my ($top, $cmd)=@_;

  # Pipe the message to the delivery agent
  my $ret=0;
  my $in=IO::Handle->new();
  my $out=IO::Handle->new();
  my $err=IO::Handle->new();
  eval {
    $SIG{'PIPE'} = 'IGNORE';
    my $pid = open3($in, $out, $err, $cmd);
    die $! if ($pid==0);
    $top->print($in) or die $!;
    close($in);
    waitpid($pid, 0);
  };
  if ($@) {
    error_log("Error while passing outgoing mail to the local delivery agent (\`$cmd\`): $@");
  }
  else {
    my $e=<$err>;
    close($err);
    close($out);
    if ($e ne "") {
      error_log("Local delivery agent error: (\`$cmd\`): $e");
    }
    else {
      $ret=1;
    }
  }
  return $ret;
}

#  my ($from, $to, $text_body, $html_body, $charset) = @_;
sub send_one_mail {
  my %args=@_;

  my $html_body=$args{html_body};

  my $decl_charset = getconf("preferred_charset", $args{from}) || 'iso-8859-1';
  my @charsets = split(/\s+/, $decl_charset);

  my ($text_body, $charset) = Manitou::MailFormat::encode_text_body($args{text_body}, @charsets);
  my $top;

  my %mime_args = (From => $args{from},
		   To => $args{to},
		   Encoding => '-SUGGEST',
		   'X-Mailer' => undef);
  if (length($text_body)>0 && length($html_body)==0) {
    $mime_args{Charset} = $charset;
    $mime_args{Data} = $text_body;
    $top = MIME::Entity->build(%mime_args);
  }
  elsif (length($html_body)>0 && length($text_body)==0) {
    $mime_args{Type} = 'text/html';
    $mime_args{Charset} = 'utf-8';
    $mime_args{Data} = $html_body;
    $top = MIME::Entity->build(%mime_args);
  }
  elsif (length($text_body)>0 && length($html_body)>0) {
    $mime_args{'Type'} = 'multipart/alternative';
    $top = MIME::Entity->build(%mime_args);

    my $p = MIME::Entity->build('Charset' => $charset,
				'Encoding' => '-SUGGEST',
				'Data' => $text_body,
			        'X-Mailer' => undef);
    $top->add_part($p);

    my $html_part = MIME::Entity->build('Data' => $html_body,
				'Encoding' => '-SUGGEST',
			        'Type' => 'text/html',
			        'Charset' => 'utf-8',
			        'X-Mailer' => undef);
    $top->add_part($html_part);
  }
  else {
    return 1;  # don't send anything if both text and html parts are empty (shouldn't happen)
  }

  Manitou::MailFormat::encode_header($top, $args{header}, @charsets);
  Manitou::MailFormat::add_date_header($top);

  my $cmd=getconf("local_delivery_agent", $args{from});
  if (!defined($cmd)) {
    print STDERR "mailing: unable to pass the mail to a local delivery agent.\nCheck your configuration file for the 'local_delivery_agent' entry\n";
    return 0;
  }
  $cmd =~ s/\$FROM\$/$args{from}/g;

  my $r=deliver_message($top, $cmd);
  $top->purge;

  return $r;
}

sub async_process_mailing {
  my ($parent_dbh, $mailing_id) = @_;
  my $pid = fork();
  if ($pid==-1) {
    die "Unable to fork process";
  }
  elsif ($pid>0) {
    return $pid;
  }
  else {
    notice_log("start of mailing process pid=$$ for mailing #$mailing_id");
    $parent_dbh->{InactiveDestroy}=1;
    my $dbh=db_connect();
    $dbh->{AutoCommit}=1;
    my ($ok,$errs);
    eval {
      ($ok,$errs)=do_mailing($dbh, $mailing_id);
    };
    if ($@) {
      error_log("mailing job failed: $@");
    }
    else {
      notice_log("mailing job ended: $ok sent, $errs in error");
    }
    exit(0);
  }
}

sub do_mailing {
  my ($dbh,$mailing_id)=@_;

  my $sth = $dbh->prepare(q{SELECT
	 throughput, header_template, text_template, html_template, sender_email, csv_columns
         FROM mailing_definition m JOIN mailing_run USING(mailing_id)
         WHERE m.mailing_id=? AND status=1});
  $sth->execute($mailing_id);
  my $row_mailing = $sth->fetchrow_hashref;
  if (!defined $row_mailing) {
    die "No entry found in mailing_definition or mailing_run for mailing_id=$mailing_id";
  }

  my $text_template = decode_dbtxt($row_mailing->{text_template});
  my $html_template = decode_dbtxt($row_mailing->{html_template});
  my $header_template = decode_dbtxt($row_mailing->{header_template});
  my @csv_columns = split /,/, decode_dbtxt($row_mailing->{csv_columns});

  my $sth1 = $dbh->prepare("SELECT recipient_email,mailing_data_id,csv_data FROM mailing_data WHERE mailing_id=? AND (sent is null OR sent='N')");
  my $sthu = $dbh->prepare("UPDATE mailing_data SET sent='Y' where mailing_data_id=?");

  my $sthu1 = $dbh->prepare("UPDATE mailing_run SET nb_sent=nb_sent+1,last_sent=now() WHERE mailing_id=?");

  my $stht = $dbh->prepare("SELECT status FROM mailing_run WHERE mailing_id=?");

  $sth1->execute($mailing_id);
  my $nb_ok=0;
  my $nb_errors=0;
  while (my $row_data = $sth1->fetchrow_hashref) {
    my $csv_line = decode_dbtxt($row_data->{csv_data});
    my ($text_body,$html_body,$header);
    # merge
    if (@csv_columns) {
      my $csvp=new Manitou::xSV;
      my ($fh,$filename) = tempfile() or die "unable to create temp file: $!";
      print $fh $csv_line;
      close $fh;
      $csvp->open_file($filename);
      $csvp->set_sep(",");
      $csvp->bind_fields(@csv_columns);

      my $fields_values = $csvp->fetchrow_hash();
      $text_body = merge_fields($text_template, $fields_values);
      $html_body = merge_fields($html_template, $fields_values);
      $header = merge_fields_header($header_template, $fields_values);
      unlink($filename);
    }
    else {
      $text_body = $text_template;
      $html_body = $html_template;
      $header = $header_template;
    }
    # send
    my $r=send_one_mail("from"=> $row_mailing->{sender_email},
			"to" => $row_data->{recipient_email},
			"text_body" => $text_body,
			"html_body" => $html_body,
			"header" => $header);
    if ($r) {
      $sthu->execute($row_data->{mailing_data_id});
      $sthu1->execute($mailing_id);
      $nb_ok++;
    }
    else {
      $nb_errors++;
    }
    # pause between messages
    select(undef,undef,undef,$row_mailing->{throughput});

    # The mailing can be stopped from the outside by setting mailing.status to 2
    # We check that before every message
    $stht->execute($mailing_id);
    my ($status)=$stht->fetchrow_array;
    if ($status!=1) {
      notice_log("mailing #$mailing_id stopped by change of status");
      last;
    }
  }
  # Updates for normal finish. If the mailing is just stopped (status=2) the queries below
  # won't update anything
  my $sth2=$dbh->prepare("UPDATE mailing_definition SET end_date=now() WHERE EXISTS (select 1 FROM mailing_run WHERE mailing_id=? AND status=1)");
  $sth2->execute($mailing_id);

  my $sth3=$dbh->prepare("UPDATE mailing_run SET status=3 WHERE mailing_id=? AND status=1");
  $sth3->execute($mailing_id);
  
  return ($nb_ok, $nb_errors);
}

1;
