# rustup target add x86_64-unknown-linux-gnu

#打包
cargo build --release 

#上传
scp ./target/release/filesystem-usage-exporter  root@192.168.124.160:/home/work/filesystem-usage-exporter/
scp ./config.yaml  root@192.168.124.160:/home/work/filesystem-usage-exporter/

#线上重启
ssh root@192.168.124.160 "systemctl restart filesystem-usage-exporter.service"
