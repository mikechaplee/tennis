#!/usr/bin/perl -w
#
use strict;

######################################
# clean up the tennis-data.co.uk csv #
######################################

my $CSV = $ARGV[0];
die "No file at '$CSV'\n" unless -f $CSV;

# we'll clean up these fields: (0-based column indices)
my $WINNER_COL = 9;
my $LOSER_COL  = 10;
my $COMMENT_COL = 27;

my %oldname2newname;
my @mutatedNameLog;
sub cleanupPlayerName($$) {
    # this function carries out several mutations
    # note that name cleanup is quite fiddly so 
    # we should log it and review
    my $origName = shift;      # don't touch this
    my $line = shift; # this is to help us log
    if (exists $oldname2newname{$origName}) {
        return $oldname2newname{$origName};
    }
    my $newName  = $origName;  # instead molest this
    # origName should already be in lower-case, but
    # just in case:
    $newName = lc($origName);
    # $newName can look like:
    # smith j.
    # smith j
    # smith j. m.
    # double barrel j.
    # double-barrel j.
    # So we need to tidy up multi-word surnames first
    # which means we need to identify where surnames
    # begin and end. Not easy at first, but we can
    # guess by using the first single-char to appear
    # (followed by either . or space or end-of-str)

    # get rid of dots and replace with a space (we
    # shouldn't replace with empty-string yet because
    # we can protect ourselves from "Smith J.M." - as
    # opposed to "Smith J. M." - by using a space and
    # then collapsing double-spaces to a single space)
    $newName =~ s/\./ /g;
    # we'll have a lot of double-spaces so collapse
    # them to one space
    $newName =~ s/\s+/ /g;

    # trim the name
    $newName =~ s/\s+$//g;

    # dutch names are sometimes annoyingly abbreviated:
    # van der <something> is sometimes presented as 
    # van d. <something>. But we've already dropped the
    # dot, so:
    $newName =~ s/van d (\S{2,})/van der $1/g;

    # finally try to hyphenate triple or double-barrel
    # surnames
    if ($newName =~ /^(\S+)\s(\S{2,})\s(\S{2,})\s(.*)/) {
        $newName = "$1-$2-$3 $4";
    } elsif ($newName =~ /^(\S+)\s(\S{2,})\s(.*)/) {
        $newName = "$1-$2 $3";
    }
    # that's enough molesting, let's now make it presentable:
    # replace all spaces with underscores
    $newName =~ s/ /_/g;
    $oldname2newname{$origName} = $newName;
    push @mutatedNameLog, "Name <$origName> converted to <$newName> at line $line";
    return $newName;
}

my %oldcomment2newcomment;
my @mutatedCommentLog;
sub cleanupComment($$) {
    # cleans up the comments. Allowed values are:
    # Completed, Retired, Walkover, Full Time, Disqualified
    # The following anomalies are observed:
    #      46 Compleed
    #       2 Sched
    #       2 Retied
    #       1 Walover
    #       1 retired
    #       1 R_Bag
    #       1 NSY
    # so we'll correct for them
    my $origComment = shift;
    my $line = shift;
    if (exists $oldcomment2newcomment{$origComment}) {
        return $oldcomment2newcomment{$origComment};
    }
    my $newComment = $origComment;
    if ($newComment =~ /compleed/i) {
        $newComment = "completed";
    } elsif ($newComment =~ /retied/i) {
        $newComment = "retired";
    } elsif ($newComment =~ /walover/i) {
        $newComment = "walkover"
    }
    $oldcomment2newcomment{$origComment} = $newComment;
    push @mutatedCommentLog, "Comment <$origComment> converted to <$newComment> at line $line";
    return $newComment;
}
    
open F, $CSV or die "Unable to open '$CSV': $!\n";
my $curLineNum = 0;
while ( <F> ) {
    $curLineNum++;
    # don't dick around with the first (header) row
    if (/^ATP,Location/ ) {
        print $_;
        next;
    }
    chomp;
    my @elems = split /,/, $_;
    # first transform: make all data lower-case
    for(my $i = 0; $i < scalar(@elems); $i++) {
        $elems[$i] = lc($elems[$i]);
    }

    # second transform: get rid of all spaces 
    # before and after a comma
    for(my $i = 0; $i < scalar(@elems); $i++) {
        $elems[$i] =~ s/^\s+//g;
        $elems[$i] =~ s/\s+$//g;
    }

    # third transform: for names of players, do cleanup
    $elems[$WINNER_COL] = cleanupPlayerName($elems[$WINNER_COL], $curLineNum);
    $elems[$LOSER_COL]  = cleanupPlayerName($elems[$LOSER_COL], $curLineNum);

    # fourth transform: cleanup the comments
    $elems[$COMMENT_COL] = cleanupComment($elems[$COMMENT_COL], $curLineNum);

    print join ",", @elems;
    print "\n";
}

foreach(@mutatedNameLog) { print "$_\n"; }
    
foreach(@mutatedCommentLog) { print "$_\n"; }

