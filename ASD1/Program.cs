using System.Diagnostics;
using ASD1;

class Program
{
    const int CHUNK_SIZE = 2000000;
    static string sourceFileName = Path.Combine(Directory.GetCurrentDirectory(), "source.txt");
    static string sortedFileName = Path.Combine(Directory.GetCurrentDirectory(), "sorted.txt");
    static string file1Name = Path.Combine(Directory.GetCurrentDirectory(), "firstTemp.bin");
    static string file2Name = Path.Combine(Directory.GetCurrentDirectory(), "secondTemp.bin");
    private const int IntOffset = sizeof(int);
    private static Dictionary<string, FileStream> fileStreams;
    private static Stopwatch appendOperation = new Stopwatch();
    private static Stopwatch readOperation = new Stopwatch();
    private static long elementsInFile;
    
    public static void Main()
    {
        //RandomFileGenerator.Generate("source", 12000000, 1, 9999999);
        bool MODIFIED = true;
        
        if (MODIFIED)
        {
            StartModifiedVersion();
        }
        else
        {
            StartDefaultVersion();
        }
        
    }

    public static void StartModifiedVersion()
    {
        string inputFile = "source.txt";  
        string tempDir = "temp";             
        string outputFile = "output.txt";  
        
        Directory.CreateDirectory(tempDir);
        Stopwatch stopwatch = new Stopwatch();
        stopwatch.Start();
        SplitFileIntoChunks(inputFile, tempDir);

        MergeChunks(tempDir, outputFile);
        stopwatch.Stop();
        Console.WriteLine(stopwatch.Elapsed);
    }
    
    public static void StartDefaultVersion()
    {
        File.Create(sortedFileName).Dispose();
        File.Create(file1Name).Dispose();
        File.Create(file2Name).Dispose();

        elementsInFile = new FileInfo(sourceFileName).Length / sizeof(int);

        Stopwatch total = new Stopwatch();
        Stopwatch separation = new Stopwatch();
        Stopwatch comparison = new Stopwatch();
        total.Start();

        FillStreamPool();

        for (int i = 1; i <= elementsInFile / 2; i *= 2)
        {
            separation.Start();
            if (i == 1)
            {
                SeparateToBinaryFiles(sourceFileName, i);
            }
            else
            {
                SeparateToBinaryFiles(sortedFileName, i);
            }

            separation.Stop();
            comparison.Start();
            ComparePairsByDefaultMethod(i);
            comparison.Stop();
            FlushFile(file1Name);
            FlushFile(file2Name);
        }

        CloseStreamPool();

        total.Stop();
        Console.WriteLine($"Separation time: {separation.Elapsed}");
        Console.WriteLine($"Comparison time: {comparison.Elapsed}");
        Console.WriteLine($"Append operation time: {appendOperation.Elapsed}");
        Console.WriteLine($"Read operation time: {readOperation.Elapsed}");
        Console.WriteLine($"Total time: {total.Elapsed}");
    }
    
    public static void SeparateToBinaryFiles(string filename, int chunkSize)
    {
        int currentFile = 1;
        FileStream fs1 = fileStreams[file1Name];
        FileStream fs2 = fileStreams[file2Name];
        BufferedStream bs1 = new BufferedStream(fs1,65536);
        BufferedStream bs2 = new BufferedStream(fs2,65536);
        
        using (StreamReader sourceReader = new StreamReader(filename))
        {
            do
            {
                for (int i = 0; i < chunkSize; i++)
                {
                    string line = sourceReader.ReadLine();

                    if (line == null)
                        break;

                    int value = int.Parse(line);
                    
                     if (currentFile == 1)
                     {
                         bs1.Write(BitConverter.GetBytes(value));
                     }
                     else if (currentFile == 2)
                     {
                         bs2.Write(BitConverter.GetBytes(value));
                     }
                }
                
                if (currentFile == 1)
                {
                    currentFile = 2;
                }
                else if (currentFile == 2)
                {
                    currentFile = 1;
                }
            } while (!sourceReader.EndOfStream);
        }
    }

    public static void FillStreamPool()
    {
        fileStreams = new Dictionary<string, FileStream>();
        fileStreams.Add(file1Name, new FileStream(file1Name, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 65536));
        fileStreams.Add(file2Name, new FileStream(file2Name, FileMode.Open, FileAccess.ReadWrite, FileShare.None, 65536));
    }

    public static void CloseStreamPool()
    {
        foreach (var fs in fileStreams)
        {
            fs.Value.Close();
        }
    }

    public static int ReadValueFromBinaryFile(string filename, int index)
    {
        readOperation.Start();
        byte[] buffer = new byte[sizeof(int)];
        int offset = IntOffset * index;
        FileStream fs = fileStreams[filename];
        fs.Seek(offset, SeekOrigin.Begin);
        fs.Read(buffer, 0, sizeof(int));
        readOperation.Stop();
        return BitConverter.ToInt32(buffer);
    }

    public static void FlushFile(string filename)
    {
        CloseStreamPool();
        File.Create(filename).Dispose();
        FillStreamPool();
    }
    
    public static void ComparePairsByDefaultMethod(int pairSize)
    {
        int file1Marker = 0;
        int file2Marker = 0;
        int file1Value = 0;
        int file2Value = 0;
        int loopCount = pairSize * 2 - 1;
        int pairNumber = 1;

        if (pairSize == 1)
        {
            loopCount = 1;
        }

        using (StreamWriter sortedWriter = new StreamWriter(sortedFileName))
        {
            do
            {
                int elementsCount = pairSize * pairNumber;
                for (int i = 0; i < loopCount; i++)
                {
                    file1Value = ReadValueFromBinaryFile(file1Name, file1Marker);
                    file2Value = ReadValueFromBinaryFile(file2Name, file2Marker);

                    if (file1Marker >= elementsCount || file2Marker >= elementsCount)
                        break;


                    if (file2Value == 0)
                    {
                        break;
                    }

                    if (file1Value < file2Value)
                    {
                        sortedWriter.WriteLine(file1Value);
                        file1Marker++;
                    }
                    else
                    {
                        sortedWriter.WriteLine(file2Value);
                        file2Marker++;
                    }
                }


                if (file1Marker < elementsCount)
                {
                    for (int i = file1Marker; i < elementsCount; i++)
                    {
                        file1Value = ReadValueFromBinaryFile(file1Name, file1Marker);
                        if (file1Value == 0)
                        {
                            break;
                        }

                        sortedWriter.WriteLine(file1Value);
                        file1Marker++;
                    }
                }
                else if (file2Marker < elementsCount)
                {
                    for (int i = file2Marker; i < elementsCount; i++)
                    {
                        file2Value = ReadValueFromBinaryFile(file2Name, file2Marker);
                        if (file2Value == 0)
                        {
                            break;
                        }

                        sortedWriter.WriteLine(file2Value);
                        file2Marker++;
                    }
                }

                pairNumber++;
            } while (ReadValueFromBinaryFile(file1Name, file1Marker) != 0 ||
                     ReadValueFromBinaryFile(file2Name, file2Marker) != 0);
        }
    }
    
    public static void SplitFileIntoChunks(string inputFile, string tempDir)
    {
        using (StreamReader sr = new StreamReader(inputFile))
        {
            List<string> chunk = new List<string>();
            int chunkIndex = 0;

            while (!sr.EndOfStream)
            {
                chunk.Clear();

                // Чтение чанка из файла
                for (int i = 0; i < CHUNK_SIZE && !sr.EndOfStream; i++)
                {
                    chunk.Add(sr.ReadLine());
                }

                // Сортировка чанка
                chunk.Sort();

                // Сохранение чанка на диск
                string chunkFile = Path.Combine(tempDir, $"chunk_{chunkIndex}.txt");
                File.WriteAllLines(chunkFile, chunk);

                chunkIndex++;
            }
        }
    }

    public static void MergeChunks(string tempDir, string outputFile)
    {
        string[] chunkFiles = Directory.GetFiles(tempDir, "chunk_*.txt");
        List<StreamReader> readers = new List<StreamReader>();

        foreach (string chunkFile in chunkFiles)
        {
            readers.Add(new StreamReader(chunkFile));
        }

        using (StreamWriter sw = new StreamWriter(outputFile))
        {
            List<string> buffer = new List<string>();
            foreach (var reader in readers)
            {
                if (!reader.EndOfStream)
                {
                    buffer.Add(reader.ReadLine());
                }
                else
                {
                    buffer.Add(null);
                }
            }

            while (true)
            {
                string smallest = null;
                int smallestIndex = -1;

                for (int i = 0; i < buffer.Count; i++)
                {
                    if (buffer[i] != null && (smallest == null || string.Compare(buffer[i], smallest, StringComparison.Ordinal) < 0))
                    {
                        smallest = buffer[i];
                        smallestIndex = i;
                    }
                }

                if (smallestIndex == -1)
                {
                    break;
                }

                sw.WriteLine(smallest);

                if (!readers[smallestIndex].EndOfStream)
                {
                    buffer[smallestIndex] = readers[smallestIndex].ReadLine();
                }
                else
                {
                    buffer[smallestIndex] = null;  
                }
            }
        }

        foreach (var reader in readers)
        {
            reader.Close();
        }
    }
}