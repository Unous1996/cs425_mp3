username="Unous1996"
netid="aol3"
password="fakepassword"
git_password="fakepassword"

git_repo="https://github.com/username/cs425_mp3"

cd ../cmd/client
go build
cd ../coordinator
go build
cd ../server
go build 
cd ../..

git commit -a -m "$(date)"
git push origin master

if sshpass -p ${password} ssh -t -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-07.cs.illinois.edu "test -d ${git_repo}"
then

sshpass -p ${password} ssh -t -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-07.cs.illinois.edu "cd /home/${netid}/mp3/script; sh server.sh"

echo ""

else

sshpass -p ${password} ssh -t -o "StrictHostKeyChecking no" ${netid}@sp19-cs425-g16-07.cs.illinois.edu "cd /home/${netid}/mp3/script; sh server.sh"

echo ""

fi


