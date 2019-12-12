#!/usr/bin/perl
use File::Copy;
use File::Spec::Functions;

$FILE_NAME_PRE='execute_parallel-pre.sql';
print "\n Output file is $FILE_NAME_PRE \n";

open($fh_out, ">", $FILE_NAME_PRE);

# get funtion defs for overlap gab 
for my $file (glob '../../../main/sql/func*') {
	copy_file_into($file,$fh_out);
}

close($fh_out);	 

sub copy_file_into() { 
	my ($v1, $v2) = @_;
	open(my $fh, '<',$v1);
	while (my $row = <$fh>) {
	  print $v2 "$row";
	}
	close($fh);	 
    
}
