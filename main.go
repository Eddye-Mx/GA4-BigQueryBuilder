package main

import (
	"fmt"
	"strings"
)

var entriesToOutputs = map[string]string{
	"Campaign": "IFNULL(traffic_source.source,'(not set)') as Campaign", /*Dimension (IS USER CAMPAIGN?)*/ 
	"Source": "IFNULL(traffic_source.source,'(not set)') as Source",/*Dimension (IS USER SOURCE?)*/
	"Medium": "IFNULL(traffic_source.medium,'not set)') as Medium",/*Dimension (IS USER MEDIUM?)*/
	"Date": "event_date as Date",/*Dimension*/
	"Sessions": "IFNULL(COUNT(case event_name when 'session_start' then 1 else null end),0) as Sessions",/*Metric*/
	"Pageviews": "IFNULL(COUNT(case event_name when 'page_view' then 1 else null end),0) as Pageviews",/*Metric*/
	"Users": "IFNULL(COUNT(DISTINCT user_pseudo_id),0) as Users",/*Metric*/
	"New_users": "IFNULL(COUNT(case event_name when 'first_visit' then 1 else null end),0) as New_users",/*Metric*/
	"Revenue": "IFNULL(SUM(ecommerce.purchase_revenue),0) as Revenue",/*Metric*/
	"Transactions": "IFNULL(COUNT(case event_name when 'purchase' then 1 else null end),0) as Transactions",/*Metric*/
	"Revenue_usd": "IFNULL(SUM(ecommerce.purchase_revenue_in_usd),0) as Revenue_usd",
	"Average_LTV": "AVG(user_ltv.revenue) as Average_LTV ",
	"item_revenue": "IFNULL(SUM(i.item_revenue),0) as item_revenue",/*Metric Not Working****/
}

var clientToDatasets = map[string]accountInfo{
	"innovasport": {
		Project: "innovasport",
		Dataset: "analytics_274438246",
	},
}

var payload = Payload{
	Accounts: []AccountData{
	   {
		  Name:"innovasport",
		  View_id:"202729775",
	   },
	},
	Dimensions:[]string{
		"Date",
	   "Source",
	   "Medium",
	   "Campaign",
	},
	Metrics:[]string{
	   "Users",
	   "New_users",
	   "Pageviews",
	   "Transactions",
	   "Revenue",
	   "Average_LTV",
	},
	Start_date:"2023-04-10",
	End_date:"2023-04-12",
}

func main() {
	theString, err := concatSQL(payload)
	if err != nil {
		fmt.Println("error: ",err)
	}
	fmt.Println(theString)

}


func concatSQL(payload Payload) (string, error){
	toReturn := "SELECT \n"
	toReturn += entriesToOutputs[payload.Dimensions[0]] 
	if(len(payload.Dimensions)>0){
		toReturn += ",\n"
		for i:= 1; i<len(payload.Dimensions); i++ {
			toReturn += entriesToOutputs[payload.Dimensions[i]] 
			toReturn += ",\n"
		}
	}
	toReturn += entriesToOutputs[payload.Metrics[0]] 
	if(len(payload.Metrics)>0){
		for i:= 1; i<len(payload.Metrics); i++ {
			toReturn += ",\n"
			toReturn += entriesToOutputs[payload.Metrics[i]] 
		}
	}
	groupBy := "\ngroup by " + payload.Dimensions[0]
	if(len(payload.Dimensions)>0){
		for i:= 1; i<len(payload.Dimensions); i++ {
			groupBy += ","
			groupBy += payload.Dimensions[i]
		}
	}
	orderBy := fmt.Sprintf("\n order by %v",(len(payload.Dimensions)+1))
	fmt.Println(groupBy)
	sDateParsed := strings.ReplaceAll(payload.Start_date, "-", "")
	eDateParsed := strings.ReplaceAll(payload.End_date, "-", "")
	ProjectInfo := clientToDatasets[payload.Accounts[0].Name]
	toReturn = fmt.Sprintf(toReturn + " FROM `%s.%s.events_*` \n where _TABLE_SUFFIX BETWEEN '%s' AND '%s' %s %s desc",ProjectInfo.Project,ProjectInfo.Dataset, sDateParsed, eDateParsed, groupBy, orderBy)

	return toReturn, nil
}


type accountInfo struct {
	Project string
	Dataset string
}

type Payload struct {
	Accounts []AccountData `json:"accounts"`
	Dimensions []string `json:"dimensions"`
	Metrics []string `json:"metrics"`
	Start_date string `json:"start_date"`
	End_date string `json:"end_date"`
}

type AccountData struct {
	Name string
	View_id string
	Property string
}