BEGIN{FS=","; Q="\""}
NR>1{n++; if(index($0,Q))q++; t=$3; gsub(Q,"",t); l=length(t); if(l==20)f1++; else if(l==25)f2++; else if(t~/^[0-9]+$/)f3++; else o++; it=$4; gsub(Q,"",it); if(length(it)==20)i1++}
END{printf "rows %d quoted-lines %.1f%% ts F1 %.1f%% F2 %.1f%% F3 %.1f%% other %.1f%% ingestF1 %.1f%%\n",n,100*q/n,100*f1/n,100*f2/n,100*f3/n,100*o/n,100*i1/n}
