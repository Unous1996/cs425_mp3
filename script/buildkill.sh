#read -p "Please enter your username:" username
#read -p "Please enter your netid:" netid
#read -sp "Please enter your password for your netid:(whatever)" password
#echo ""
#read -sp "Please enter your github password:(whatever)" git_password
#echo ""

username="Unous1996"
netid="aol3"
password="fakepassword"
git_password="fakepassword"

git_repo="https://github.com/username/cs425_sp19_mp2"

for i in {2..10};
do

if ((i < 10))
then

sshpass -p ${password} ssh -t -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-0${i}.cs.illinois.edu "cd /home/${netid}/mp3; killall -s SIGKILL main client; killall -s SIGKILL coordinator; killall -s SIGKILL server; cd cmd/client; go build; cd ../coordinator; go build; cd ../server; go build;"

else

# sshpass -p ${password} ssh -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-${i}.cs.illinois.edu "hostname"
sshpass -p ${password} ssh -t -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-${i}.cs.illinois.edu "cd /home/${netid}/mp3; killall -s SIGKILL main client; killall -s SIGKILL coordinator; killall -s SIGKILL server; cd cmd/client; go build; cd ../coordinator; go build; cd ../server; go build;"

fi

done
