const fs = require('fs');
const readline = require('readline');
const { promisify } = require('util');

const CHUNK_SIZE = 500 * 1024 * 1024; // 500 MB

const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);
const appendFileAsync = promisify(fs.appendFile);

async function sortLargeFile(filename) {
  // Создание временной папки для хранения промежуточных фрагментов
  const tempFolder = './temp';
  if (!fs.existsSync(tempFolder)) {
    fs.mkdirSync(tempFolder);
  }

  // Разделение исходного файла на фрагменты
  const chunkFiles = [];
  let currentChunk = [];
  let currentChunkSize = 0;

  const readStream = fs.createReadStream(filename);
  const rl = readline.createInterface({
    input: readStream,
    crlfDelay: Infinity,
  });

  for await (const line of rl) {
    currentChunk.push(line);
    currentChunkSize += line.length + 1; // +1 for newline character

    if (currentChunkSize >= CHUNK_SIZE) {
      const chunkFile = `${tempFolder}/chunk_${chunkFiles.length}.txt`;
      await writeFileAsync(chunkFile, currentChunk.join('\n'));
      chunkFiles.push(chunkFile);

      currentChunk = [];
      currentChunkSize = 0;
    }
  }

  // Обработка последнего фрагмента, если он есть
  if (currentChunk.length > 0) {
    const chunkFile = `${tempFolder}/chunk_${chunkFiles.length-1}.txt`;
    await writeFileAsync(chunkFile, currentChunk.join('\n'));
    chunkFiles.push(chunkFile);
  }

  // Сортировка каждого фрагмента в памяти
  const sortedChunkFiles = [];
  for (const chunkFile of chunkFiles) {
    const lines = await readFileAsync(chunkFile, 'utf8');
    const sortedLines = lines.split('\n').sort();
    const sortedChunkFile = `${tempFolder}/sorted_${chunkFile.split('/').pop()}`;
    await writeFileAsync(sortedChunkFile, sortedLines.join('\n'));
    sortedChunkFiles.push(sortedChunkFile);
  }

  // Слияние отсортированных фрагментов в один файл
  const outputFilename = 'sorted_large_file.txt';
  for (const sortedChunkFile of sortedChunkFiles) {
    await appendFileAsync(outputFilename, await readFileAsync(sortedChunkFile, 'utf8'));
  }

  // Удаление временных файлов и папки
  for (const chunkFile of chunkFiles) {
    fs.unlinkSync(chunkFile);
  }
  for (const sortedChunkFile of sortedChunkFiles) {
    fs.unlinkSync(sortedChunkFile);
  }
  fs.rmdirSync(tempFolder);

  console.log(`Файл ${filename} успешно отсортирован и сохранен в ${outputFilename}.`);
}

// Запуск программы
const filename = 'large_file.txt';
sortLargeFile(filename);
