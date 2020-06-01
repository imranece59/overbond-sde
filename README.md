**Problem Statement:**

Given a JSON file which will define an array of bond objects (of arbitrary size), write a command line tool to calculate the spread between each corporate bond and the nearest government bond benchmark, save these results in a JSON file, and express the spread in basis points, or bps. If any properties are missing from a bond object, do not include it in your calculations or output.

Spread is defined as the difference between the yield of a corporate bond and its government bond benchmark.

A government bond is a good benchmark if it is as close as possible to the corporate bond in terms of years to maturity, also known as term or tenor.

If there is a tie for closest government bond by tenor, break the tie by choosing the government bond with the largest amount outstanding.

To convert your difference to basis points, just scale your spread by 100 and display as an integer (truncate trailing decimals), e.g. if your spread comes out to 2.127, this will be expressed in your output file as "212 bps".

Sample input
{
  "data": [
    {
      "id": "c1",
      "type": "corporate",
      "tenor": "10.3 years",
      "yield": "5.30%",
      "amount_outstanding": 1200000
    },
    {
      "id": "c1",
      "type": "corporate",
      "tenor": "10.3 years",
      "yield": "6.50%",
      "amount_outstanding": 1300000
    },
    {
      "id": "g1",
      "type": "government",
      "tenor": "9.4 years",
      "yield": "3.70%",
      "amount_outstanding": 2500000
    },
    {
      "id": "c2",
      "type": "corporate",
      "tenor": "13.5 years",
      "yield": null,
      "amount_outstanding": 1100000
    },
    {
      "id": "g2",
      "type": "government",
      "tenor": "12.0 years",
      "yield": "4.80%",
      "amount_outstanding": 1750000
    }
  ]
}

Sample output
{
  "data": [
    {
      "corporate_bond_id": "c1",
      "government_bond_id": "g1",
      "spread_to_benchmark": "280 bps"
    }
  ]
}
Explanation
Each output object in the list represents a pairing of one corporate bond to its closest government bond benchmark, and the spread between their yields.

The best benchmark for bond c1 is g1, since the absolute difference in their terms (|10.3 - 9.4|) is only 0.9, but comparing c1and g2 gets you 1.7. The spread is calculated as simply the corporate yield - government yield, you would obtain 5.30 - 3.70 = 1.60, which you must represent in basis points as "160 bps".

The bond c2 is not included in the output because it is missing a property, yield. If any properties are missing from a bond object, do not include it in the calculation and output. You may assume you will always have at least one valid government bond and at least one valid corporate bond, for all inputs.

**software requirements**
Spark 2.2
Scala 2.11
hadoop 2.7+
sbt

**Design Approach:**

1. Load the json file using spark read json. make sure to enable multiline to true while reading

2. Flatten the rows in the below format

+---+----------+----------+-----+------------------+
|id |type      |tenor     |yield|amount_outstanding|
+---+----------+----------+-----+------------------+
|c1 |corporate |10.3 years|5.30%|1200000           |
|c1 |corporate |10.3 years|6.50%|1300000           |
|g1 |government|9.4 years |3.70%|2500000           |
|c2 |corporate |13.5 years|null |1100000           |
|g2 |government|12.0 years|4.80%|1750000           |
+---+----------+----------+-----+------------------+

3. drop out the records where any of the property is null for bonds and separate the corporate & government bonds to different
dataframe. Then calculate the absolute difference in terms (if tie comes, take the record which has largest_outstanding amount)

refer:- calculateBestbenchMarkDf method in ProcessDataHelper class
 
4. Calculate the spread and convert the dataframe to json dataset. Then write the final dataset to output path



**Running**

- sbt "runMain demo.common.BondSpreadAnalysis --input-file-path=C:\exam\sde-test\sample_input.json --output-file-path=file:///C:/exam/sde-test/output.json  --master=local[*]"


**Sample Output**

|value                                                                                            |
+-------------------------------------------------------------------------------------------------+
|{"data":[{"corporate_bond_id":"c1","government_bond_id":"g1","spread_to_benchmark":"280.0 bps"}]}|
+-------------------------------------------------------------------------------------------------+

![Screenshot](Capture.png)

**Eclipse Build**

- Git clone to project
- run sbt eclipse from the project folder
- import the project into ScalaIde/IntelliJ


**Notes**
This is a pure Spark/Scala program which run on local mode. 
- if you face any error while writing the final dataset to output path you must be missing the correct hadoop version or setting winutils required for 
- hadoop write 
- You dont need winutils setup if you run from EMR or cluster without master as local[*]


 
 
 
 
 
 


