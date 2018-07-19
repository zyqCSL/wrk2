# rps=(50k 100k 150k 200k 250k 300k 350k 400k 450k 500k 550k 600k 650k 700k 750k 800k 850k 900k 950k 1000k)
# rps=(50k 100k 150k 200k 250k 300k 350k 400k 450k 500k 550k 600k 650k 700k 750k 800k)
# rps=(50k 100k 150k 200k 250k 300k 350k 400k 450k 500k 550k 600k 650k 700k)
# rps=(50k 100k 150k 200k 250k 300k 350k 400k)
# rps=(50k 100k 150k 200k 250k 300k 350k)
rps=(20k 40k 60k 80k 100k 120k 140k 160k 180k 200k)
#rps=(40k 60k 80k 100k 120k 140k 160k 180k 200k)
#rps=(10k 20k 30k 40k 50k 60k 70k 80k 90k 100k)
#rps=(20k 40k 60k 80k 100k 120k 140k 160k 180k)
targ_machine=$1
rounds=$2

declare -A ip_addr
ip_addr["ath1"]="128.253.128.64"
ip_addr["ath2"]="128.253.128.65"
ip_addr["ath3"]="128.253.128.66"
ip_addr["ath4"]="128.253.128.67"
ip_addr["ath5"]="128.253.128.68"
ip_addr["ath6"]="128.253.128.69"
ip_addr["ath7"]="128.253.128.70"
ip_addr["ath8"]="128.253.128.76"

dir='/home/yz2297/3tier_rapl/2018_5_22/3tier_test_8p_nginx_2.6Ghz_ath2_4t_memc_ath5_1.2Ghz/test/'
mkdir -p dir
for i in {0..9}
do
	rps_dir=$dir'/qps_'${rps[$i]}
	rps=${rps[$i]}
	mkdir -p $rps_dir
	rid=1
	while [[ $rid -le $rounds  ]]
	do
		echo $rps_dir'/round_'$rid
		echo $rps 
		sudo nice -n -20 taskset -c 4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19 ./wrk -t 16 -c 320 -d 60s -s scripts/multiple-url-paths.lua  -R $rps http://${ip_addr["$targ_machine"]}:8088 > $rps_dir'/round_'$rid
		((rid=rid+1))
	done
done
