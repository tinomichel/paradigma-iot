#!/usr/bin/perl
use strict;
use warnings;

use Getopt::Std;
use Time::Local;
use IO::Socket::INET;
use InfluxDB::LineProtocol qw(data2line precision=ms);
use Switch;
use DateTime;
use DateTime::Duration;
use DateTime::Format::Strptime;
use Date::Format;
use Hijk;
use Data::Dumper;
use Log::Dispatch;
use Log::Dispatch::File;
use File::Fetch;
use URI::Escape qw(uri_escape);

use constant LOG_DIR    => '/var/log/paradigma';
use constant LOG_FILE   => 'paradigma.log';

my $systalan_ip = '10.5.6.76';
my $systalan_port = '7260';
my $systalan_pw = '31323334';
my $SocketTimeOut  = 120; # seconds, must be > 60
my $MaxDataLength  = 2048;

my $Usage = qq{
Usage: $0 [options]

Options: -d       dump data bytes
         -D       print debug output
         -v       be verbose
         -w COLS  print COLS columns when dumping (default is 8)
};
our ($opt_d, $opt_D, $opt_v, $opt_w);

getopts("dDvw:") || die $Usage;

our $Dump    = $opt_d;
our $Debug   = $opt_D;
our $Verbose = $opt_v;
our $Columns = $opt_w || 8;

#logging
my $log = new Log::Dispatch(
      callbacks => sub { my %h=@_; return Date::Format::time2str('%B %e %T', time)." $0\[$$]: ".$h{message}."\n"; }
);
$log->add( Log::Dispatch::File->new( name      => 'file1',
                                     min_level => 'debug',
                                     mode      => 'append',
                                     filename  => File::Spec->catfile( LOG_DIR, LOG_FILE),
                                   )
);

sub Main {
	$log->warning("Starting Processing:  ");

	my $keepgoing = 1;

	$SIG{HUP}  = sub { $log->warning("Caught SIGHUP:  exiting gracefully") ; $keepgoing = 0; };
	$SIG{INT}  = sub { $log->warning("Caught SIGINT:  exiting gracefully") ; $keepgoing = 0; };
	$SIG{QUIT} = sub { $log->warning("Caught SIGQUIT:  exiting gracefully" ); $keepgoing = 0; };

	my $Socket = new IO::Socket::INET(
		PeerHost => $systalan_ip,
		PeerPort => $systalan_port,
		Proto     => "udp",
		) or die "ERROR in socket creation: $!\n";

	my $lastmin = (localtime())[1];
	$lastmin--;

	my $LastPacket = 0;
	## MAIN loop
	while ($keepgoing) {
		my $now = (localtime())[1];
		if ($lastmin ne $now) {
			$lastmin = $now;
			SendMonitorCommand($Socket);
		};

	        my @Data = Receive($Socket);
		# only process each 5 secs
		if ( (time() - $LastPacket > 5) && @Data) {
           		$LastPacket = time() if Process(@Data);
           	}
        	Check($LastPacket && (time() - $LastPacket > $SocketTimeOut), "Kein Datenpaket empfangen!", "Datenpaket empfangen.", "MailNoPacket");
        }

	$Socket->close();

}

sub SendMonitorCommand {
	my $Socket = shift;

	my $command = $systalan_pw . "0a0114e1";
	my @data = map { hex($_) } ($command =~ /(..)/g);
	Send ($Socket, @data);

}

sub Receive {
	my $Socket = shift;
	my $s = "";
	my $TimedOut = 0;
	eval {
	    local $SIG{ALRM} = sub { $TimedOut = 1; };
	    alarm($SocketTimeOut);
	    $Socket->recv($s, $MaxDataLength);
	    alarm(0);
	    1;
	    };
	return () unless Check($TimedOut, "Keine Verbindung!", "Verbindung OK.", "MailLostConnection");
	if ($Verbose) {
		my $logmsg = sprintf("received %d bytes from %s:%s", length($s), $Socket->peerhost(), $Socket->peerport());
		$log->info ($logmsg);
	}
	my @a = StringToArray($s);
	Dump(@a) if ($Dump);
	return @a;
}

sub Send {
	my ($Socket, @a) = @_;
	if ($Verbose) {
		my $logmsg = sprintf("sending %d bytes to %s:%s", $#a + 1, $Socket->peerhost(), $Socket->peerport());
		$log->info ($logmsg);
	}

	Dump(@a) if ($Dump);
	$Socket->send(ArrayToString(@a));
}

sub ArrayToString {
	my @bytes = (@_);

	return join '', map chr, @bytes;
}

sub StringToArray {
  my $s = shift;
  my @a = ();
  my $l = length($s);
  for (my $i = 0; $i < $l; $i++) {
      $a[$i] = ord(substr($s, $i, 1));
      }
  return @a;
#	return unpack('C*',$s);
}


sub Dump {
  my @a = @_;
  my $logmsg="";
  for (my $i = 0; $i <= $#a; $i++) {
      $logmsg .= sprintf("\n") if ($i && ($i % $Columns) == 0);
      $logmsg .= sprintf(" %02X", $a[$i]);
      }
  $logmsg .= sprintf("\n");
  $log->info ($logmsg);
}

sub Check
{
  my ($ErrorCondition, $ErrorMsg, $OkMsg, $LastReported) = @_;
  return !$ErrorCondition;
}

sub Process
{
  my @a = @_;
  # 0..5: MAC address of paradigma control board:
  @Mac = ($a[3], $a[2], $a[1], $a[0], $a[5], $a[4]);
  # 6..7: counter, incremented by 1 for each packet
  # 8..15: always "09 09 0C 00 32 DA 00 00"
  # 16: packet type (00 = empty intial packet, 01 = actual data packet, 02 = short final packet)
  return 0 unless $a[16] == 0x01; # we're only interested in the actual data
  # 17..23: always "00 00 00 00 00 00 00"
  # Everything from offset 24 upwards are 4 byte integers:
  my @n = AtoInt(splice(@a, 24));

  #  26: $InnenSoll2 = 0; # Raumtemperatur (soll) Heizkreis 2
  #  78: $Trn2 = 0; # Raumtemperatur normal (soll) Heizkreis 2
  #  79: $Trk2 = 0; # Raumtemperatur komfort (soll) Heizkreis 2
  #  80: $Tra2 = 0; # Raumtemperatur abgesenkt (soll) Heizkreis 2
  #  75: $BA2 = 0; # Betriebsart Heizkreis 2

  # The following is for packet type 01 only:
  $Aussen           = $n[0] / 10;  # Aussentemperatur
  $HeizungVorlauf   = $n[1] / 10;  # Vorlauftemperatur Heizung (ist)
  $HeizungRuecklauf = $n[2] / 10;  # Ruecklauftemperatur Heizung
  $Brauchwasser     = $n[3] / 10;  # Brauchwassertemperatur (ist)
  $TPO              = $n[4] / 10;  # Speichertemperatur oben
  $TPU              = $n[5] / 10;  # Speichertemperatur unten
  $Zirkulation      = $n[6] / 10;  # Zirkulation (ist)
  $HK2VLi           = $n[7] / 10;  # Heizkreis 2 Vorlauf (ist)
  $HK2RLi           = $n[8] / 10;  # Heizkreis 2 Rücklauf (ist)
  $Raumtemperatur   = $n[9] / 10;  # Raumtemperatur (ist)
  $tsa              = $n[11] / 10; # Kollektortemperatur (ist)
  $KesselVorlauf    = $n[12] / 10; # Vorlauftemperatur Kessel (ist)
  $tsv              = $n[13] / 10; # Ruecklauftemperatur Kessel
  $tam              = $n[14] / 10; # Aussentemperatur Dach
  $tse              = $n[16] / 10; # Solarruecklauf
  $flow             = $n[17] / 10; # Durchfluss
  $pso              = $n[18];      # pso
  $BrauchwasserSoll = $n[22] / 10; # Brauchwassertemperatur (soll)
  $InnenSoll        = $n[23] / 10; # Raumtemperatur (soll)
  $24               = $n[24] / 10; # 24 ? Heizkreislauf?
  $HK1VLs           = $n[25] / 10; # Heizkreis 1 Vorlauf (soll)
  $HK2VLs           = $n[28] / 10; # Heizkreis 2 Vorlauf (soll)
  $KesselSoll       = $n[34] / 10; # angeforderte Kesseltemperatur
  $BA  = $n[36]; # Betriebsart (0 = Auto Prog. 1, 6 = Sommer)
                 # 37: wie 36?
  $Trn = $n[39]; # Raumtemperatur normal (soll)
  $Trk = $n[40]; # Raumtemperatur komfort (soll)
  $Tra = $n[41]; # Raumtemperatur abgesenkt (soll)
                 # 42: Status?
                 # 47: Regelung HK nach: 0=Aussentemperatur, 1=Raumtemperatur, 2= TA/TI kombiniert
  $Fusspunkt1       = $n[48] / 10; # Fusspunkt HK1
                 # 49: Heizkurvenoptimierung?
  $Steilheit1       = $n[50] / 10; # Steilheit HK1
                 # 51: Heizkurvenoptimierung?
  $TVm = $n[52]; # Max. Vorlauftemperatur
  $HeizgrenzeHeizen = $n[53] / 10; # Heizgrenze Heizbetrieb
  $HeizgrenzeAbsenken = $n[54] / 10; # Heizgrenze Absenken
  $Tfs = $n[55]; # Frostschutz Aussentemperatur
  $tva = $n[56]; # Vorhaltezeit Aufheizen
  $Raumeinfluss = $n[57] / 10; # Raumeinfluss
  $uek = $n[58]; # Ueberhoehung Kessel
  $shk = $n[59]; # Spreizung Heizkreis
  $phk = $n[60]; # Minimale Drehzahl Pumpe PHK
  $tmi = $n[62]; # Mischer Laufzeit
                 # 65: Raumtemperatur Abgleich (* 10, neg. Werte sind um 1 zu hoch, 0 und -1 werden beide als 0 geliefert)
  $Fusspunkt2       = $n[87] / 10; # Fusspunkt HK1
                                # 49: Heizkurvenoptimierung?
  $Steilheit2       = $n[89] / 10; # Steilheit HK1
  $TWn = $n[149]; # Brauchwassertemperatur normal
  $TWk = $n[150]; # Brauchwassertemperatur komfort
  $Stx = $n[151]; # Status?
  $BrauchwasserDelta = $n[155] / 10; # Brauchwasser Schaltdifferenz
  $npp = $n[158]; # Nachlauf Pumpe PK/LP
  $tmk = $n[162]; # Min. Laufzeit Kessel
                  # 179: Betriebszeit Kessel (Stunden)
                  # 180: Betriebszeit Kessel (Minuten)
                  # 169: Nachlaufzeit Pumpe PZ
  $NrB = $n[181]; # Anzahl Brennerstarts
  $ZirkulationDelta = $n[171]; # Zirkulation Schaltdifferenz
                  # 183: Solargewinn Tag???
                  # 184: Solargewinn gesamt???
  $SolareLeistungKW = $n[182] / 10;
  $SolareLeistungTag = $n[183];
  $SolareLeistungGesamt = $n[184];
  $Countdown = $n[186]; # some countdown during the night?!
  $Relais = $n[220]; # aktive Relais
  $Heizkreispumpe    = int(($Relais & $RelaisHeizkreispumpe) != 0);
  $Ladepumpe         = int(($Relais & $RelaisLadepumpe) != 0);
  $Zirkulationspumpe = int(($Relais & $RelaisZirkulationspumpe) != 0);
  $Kessel            = int(($Relais & $RelaisKessel) != 0);
  $Brenner           = $Kessel && ($KesselVorlauf - $KesselRuecklauf > 2);
                  # 222: Status?
  $Err = $n[228]; # Fehlerstatus (255 = OK)
  $Fehler = int($Err != $NoError); # w/o the int() it is an empty string instead of 0!?
                  # 230: Status?
                  # 231: Status?
  $St  = $n[232]; # Status
                  # 248: Status?

	my %vals;
	$vals{tsa} = $tsa;
	$vals{tw} = $TPO;
	$vals{tsv} = $tsv;
	$vals{tam} = $tam;
	$vals{tse} = $tse;
	$vals{flow} = $flow;
	$vals{pso} = $pso;
	$vals{status} = $St;
	$vals{tagesleistung} = $SolareLeistungTag;
	$vals{gesamt} = $SolareLeistungGesamt;

	my $line = data2line(
                'solardaten',
                \%vals,
                {},
		,
                );

	$log->debug ("data sent to influx: " . $line);

        my $res = Hijk::request({
        method       => "POST",
        host         => "127.0.0.1",
        port         => "8086",
        path         => "/write",
        query_string => "db=solardaten&precision=ms&u=kwb&p=kwb",
        body         => $line
        });



  return 1;
}

sub AtoInt
{
  my @a = @_;
  my @n = ();
  for (my $i = 0; $i < $#a; $i += 4) {
      my $t = $a[$i] + ($a[$i + 1] << 8) + ($a[$i + 2] << 16)+ ($a[$i + 3] << 24);
      $t -= 0xFFFFFFFF if (($a[$i + 3] & 0x80) != 0);
      push(@n, $t);
      }
  return @n;
}

Main();
