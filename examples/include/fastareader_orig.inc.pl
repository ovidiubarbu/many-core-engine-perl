
use strict;
use warnings;

package BioUtil::Seq;

no strict 'refs';
no warnings 'redefine';

## Below, original FastaReader from the CPAN BioUtil-2014.1226 module.
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
        open $fh, "<", $file
            or die "fail to open file: $file!\n";
    }

    return sub {
        if ($finished) {    # end of file
            return undef;
        }

        while (<$fh>) {
            s/^\s+//;       # remove the space at the front of line

            if (/^>(.*)/) { # header line
                ( $header, $last_header ) = ( $last_header, $1 );
                ( $seq,    $seq_buffer )  = ( $seq_buffer,  '' );

                # only output fasta records with non-blank header
                if ( $header ne '' ) {
                    $seq =~ s/\s+//g unless $not_trim;
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
            $seq_buffer =~ s/\s+//g unless $not_trim;
            return [ $last_header, $seq_buffer ];
        }
    };
}

1;

