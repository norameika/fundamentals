open(IN, 'customer_data.csv');
while(<IN>){
    chomp;
    my @d = split(/,/, $_);
    # ------------------------
    # ここに、集計処理を書く
    # ------------------------
}
close(IN);