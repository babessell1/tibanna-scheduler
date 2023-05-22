curl https://nim-lang.org/choosenim/init.sh -sSf > init.sh && sh init.sh
echo 'export PATH=/home/ubuntu/.nimble/bin:$PATH' >> ~/.bashrc
source ~/.bashrc

git clone https://github.com/quinlan-lab/STRling.git
cd STRling
nimble install

nim c -d:danger -d:release src/strling.nim
cd ../