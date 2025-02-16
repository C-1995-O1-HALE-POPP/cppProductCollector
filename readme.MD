
# cpp Event Crawler

## Overview

cppEventCrawler is a tool that analyzes the published data from [allcpp](https://www.allcpp.cn/), a exhibition management platform. 

## Sources

The tool automatically collects published with api from:

- **Event Pages**: [sample](https://www.allcpp.cn/allcpp/event/event.do?event=2231) - Retrieve all products and participating circles within an event. 
- **Circle Pages**: [sample](https://www.allcpp.cn/c/5096.do) - Extract detailed circle information, exhibition history, and member data.  
- **User Pages**: [sample](https://www.allcpp.cn/u/5043.do?showtype=1) - Gather activity records, personal profiles, and created works.  
- **Product Details**: [sample](https://www.allcpp.cn/d/984953.do#tabType=0) - Obtain comprehensive metadata for each product.  

## Task Workflow

1. **Extract event data**:  
   - Retrieve all participating circles and registered products from the event homepage.  

2. **Process all products**:  
   - Fetch detailed product information.  
   - Record product exhibition history.  

3. **Process all circles (booths)**:  
   - Extract circle details and their exhibition records.  
   - Identify and list all participating members.  

4. **Process all participants (users)**:  
   - Collect detailed user information.  
   - Retrieve individual exhibition history.  
   - Gather works created by each participant.  

This structured approach ensures complete data coverage for each event.

## Dependencies

This project uses utility functions from [cppTickerBuy](https://github.com/mikumifa/cppTickerBuy) for logging and request handling.

## Input

- **Event ID**
- or **Event URL** (e.g., `https://www.allcpp.cn/allcpp/event/event.do?event={eventId}`)

## Output

The output consists of CSV files containing detailed data extracted from the event, including:

- **Event products and circles**
- **Circles information, published schedules and information**
- **Product information and schedule**
- **Published user information and their published activity schedules**

Example output files:
```
{eventId}_Circle_ALL_Products.csv
{eventId}_Circle_Schedule.csv
{eventId}_Circles_Info.csv
{eventId}_Event_circles.csv
{eventId}_Event_products.csv
{eventId}_Product_Schedule.csv
{eventId}_Products_Info.csv
{eventId}_User_ALL_Products.csv
{eventId}_User_Info.csv
{eventId}_User_Schedule.csv
```

## Future Improvements (TODO)

- **Flexible Task Management**
- **Dataset Translation**
