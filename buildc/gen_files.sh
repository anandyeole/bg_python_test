for i in {1..200}
do 
	echo "$(cat src.c)" > ${i}.c 
	echo ${i}.c > ${i}.txt
done

