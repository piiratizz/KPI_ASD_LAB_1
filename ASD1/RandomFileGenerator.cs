namespace ASD1;

public class RandomFileGenerator
{
    public static void Generate(string fileName, int numbersCount, int rangeMin, int rangeMax)
    {
        string path = Directory.GetCurrentDirectory() + "/" + fileName + ".txt";
        
        File.Create(path).Dispose();
        
        using (StreamWriter sw = new StreamWriter(path))
        {
            Random rand = new Random();
            for (int i = 0; i < numbersCount; i++)
            {
                int num = rand.Next(rangeMin, rangeMax);
                sw.WriteLine(num);
            }
        }
    }
}