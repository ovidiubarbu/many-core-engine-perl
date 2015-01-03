
use strict;
use warnings;

package BioUtil::Seq;

no strict 'refs';
no warnings 'redefine';

## Below, FastaReader from the CPAN BioUtil-2014.1226 module with slight mods.
## The default input record separator is used, thus line driven.

sub FastaReader {
    my ( $file, $not_trim ) = @_;

    my ( $last_header, $seq_buffer ) = ( '', '' ); # buffer for header and seq
    my ( $header,      $seq )        = ( '', '' ); # current header and seq
    my $finished = 0;

    my $fh       = undef;
    my $is_stdin = 0;

    if ( $file =~ /^STDIN$/i ) {
        $fh       = *STDIN;
        $is_stdin = 1;
    }
    else {
        if (ref $file eq '' || ref $file eq 'SCALAR') {
            open $fh, '<', $file or die "fail to open file: $file!\n";
        } else {
            $fh = $file;
        }
    }

    return sub {
        if ($finished) {    # end of file
            return undef;
        }

        while (<$fh>) {
          # s/^\s+//;       # remove the space at the front of line

            if (/^>(.*)/s) { # header line including \r?\n
                ( $header, $last_header ) = ( $last_header, $1 );
                ( $seq,    $seq_buffer )  = ( $seq_buffer,  '' );

                # only output fasta records with non-blank header
                if ( $header ne '' ) {
                    unless ($not_trim) {
                       chop $header;
                       chop $header if substr($header, -1, 1) eq "\r";
                       $seq =~ tr/ \t\r\n//d;
                    }
                    return [ $header, $seq ];
                }
            }
            else {
                $seq_buffer .= $_;    # append seq
            }
        }
        close $fh unless $is_stdin;
        $finished = 1;

        # last record
        # only output fasta records with non-blank header
        if ( $last_header ne '' ) {
            unless ($not_trim) {
               chop $last_header;
               chop $last_header if substr($last_header, -1, 1) eq "\r";
               $seq_buffer =~ tr/ \t\r\n//d;
            }
            return [ $last_header, $seq_buffer ];
        }
    };
}

1;

