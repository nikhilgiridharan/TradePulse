#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

// Struct to hold stock data
struct StockData {
    std::string index;
    std::string date;
    double open;
    double high;
    double low;
    double close;
    double adjClose;
    std::string volume;
    double closeUSD;
};

// Function to parse date into YYYY-MM-DD format
std::string standardizeDate(const std::string& date) {
    // Example input: "31/12/86" -> "1986-12-31"
    std::stringstream ss(date);
    std::string day, month, year;
    getline(ss, day, '/');
    getline(ss, month, '/');
    getline(ss, year, '/');
    return "19" + year + "-" + month + "-" + day; // Assuming all dates are in the 1900s
}

// Function to clean and process stock data
void cleanStockData(const std::string& inputFile, const std::string& outputFile) {
    std::ifstream inFile(inputFile);
    std::ofstream outFile(outputFile);
    std::string line;
    
    // Write headers to output file
    outFile << "Index,Date,Open,High,Low,Close,Adj Close,Volume,CloseUSD\n";

    // Read and process each line
    while (getline(inFile, line)) {
        std::stringstream ss(line);
        std::string token;
        StockData stock;

        // Parse data fields
        getline(ss, stock.index, ',');
        getline(ss, stock.date, ',');
        stock.date = standardizeDate(stock.date); // Standardize date
        getline(ss, token, ',');
        stock.open = std::stod(token);
        getline(ss, token, ',');
        stock.high = std::stod(token);
        getline(ss, token, ',');
        stock.low = std::stod(token);
        getline(ss, token, ',');
        stock.close = std::stod(token);
        getline(ss, token, ',');
        stock.adjClose = std::stod(token);
        getline(ss, stock.volume, ',');
        if (stock.volume == "0") stock.volume = "N/A"; // Replace zero volume
        getline(ss, token, ',');
        stock.closeUSD = std::stod(token);

        // Write cleaned data to output file
        outFile << stock.index << "," << stock.date << "," << stock.open << ","
                << stock.high << "," << stock.low << "," << stock.close << ","
                << stock.adjClose << "," << stock.volume << "," << stock.closeUSD << "\n";
    }

    inFile.close();
    outFile.close();
    std::cout << "Cleaned data saved to " << outputFile << std::endl;
}

int main() {
    cleanStockData("stockData.csv", "cleaned_stockData.csv");
    return 0;
}
