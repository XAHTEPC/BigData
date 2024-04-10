package practise_three;

import practise_three.methods.*;

import java.util.Scanner;

public class Main {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        int i=0;
        while (i==0){
            System.out.println("Enter task number: " +
                    "\n 1: select * from transport join owner on transport.id = owner.id" +
                    "\n 2: select * from transport left join color on transport.id = color.id;" +
                    "\n 3: select * from transport right join city on transport.id = city.id;" +
                    "\n 4: select c,count(c) from color group by c;" +
                    "\n 5: select * from city where town='Tula'; " +
                    "\n 6: exit ");
            String id = scanner.next();
            if(id.equals("1")){
                Task1.start();
            }
            if(id.equals("2")){
                Task2.start();
            }
            if(id.equals("3")){
                Task3.start();
            }
            if(id.equals("4")){
                Task4.start(); //ok
            }
            if(id.equals("5")){
                Task5.start(); //ok
            }
            if(id.equals("6")){
                i=1;    //ok
            }
        }
    }



}