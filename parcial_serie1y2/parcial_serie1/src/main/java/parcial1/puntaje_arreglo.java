package parcial1;

public class puntaje_arreglo {


public static int score(int[] numbers) {

    int total = 0;

    for (int i = 0; i < numbers.length; i++ ) {

        int n = numbers[i];

        if (n == 5) {
            total = total + 5;
        } 
        else if (n % 2 == 0) {
            total = total + 1;
        } 
        else {
            total = total + 3;
        }

    }

    return total;
}

}