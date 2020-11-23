package reorg

import (
	"strings"
	"testing"
	"time"

	"github.com/alpacahq/marketstore/v4/contrib/ice/enum"

	. "gopkg.in/check.v1"
)

func TestAnnouncement(t *testing.T) { TestingT(t) }

type TestAnnouncementSuite struct {
}

var _ = Suite(&TestAnnouncementSuite{})

func date(year int, month time.Month, day int) time.Time {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC)
}

func (t *TestAnnouncementSuite) TestReadRecord(c *C) {
	input := `........................................................... ENT:01/24/20:(2020) 
DEAL#: 1: TEXT#:2052662: REMARKS:STOCK SPLIT :   IND:7N  C: EFF:99/99/99:(9999) 
TARGET:928534106: :VIVIC CORP: INIT:999999999: :9999999999: EXP:01/27/20:(2020) 
CASH:   0.00000:  : : PROTECT:  :  T.AGENT:    : INFO:    : WDR:01/20/20:(2020) 
RATE:   4.00000:  : : :  1.00000:M:DTC: : PRATE:  0.000000: PRO:01/24/20:(2020) 
"VIVIC CORP"                                                                    
COM                                                                             
DECLARED A 4 FOR 1 STOCK SPLIT                 NEW RATE:     4.000000:          
DECLARATION DATE: NOT REPORTED                 OLD RATE:     1.000000:          
EX-DATE:          JANUARY 27, 2020                                              
RECORD DATE:      JANUARY 20, 2020                                              
PAYABLE DATE:     JANUARY 24, 2020                                              
DUE BILL REDEMPTION DATE:  /  /  :                                  **          
`
	lines := strings.Split(input, "\n")
	a := Announcement{}
	readRecord(lines, &a)

	c.Assert(a.EntryDate, DeepEquals, date(2020, time.January, 24))
	c.Assert(a.DealNumber, Equals, "1")
	c.Assert(a.TextNumber, Equals, 2052662)
	c.Assert(a.Remarks, Equals, "STOCK SPLIT")
	c.Assert(a.NotificationType, Equals, enum.StockSplit)
	c.Assert(a.Status, Equals, enum.NewAnnouncement)
	c.Assert(a.UpdatedNotificationType, Equals, enum.UnsetNotification)
	c.Assert(a.NrOfOptions, Equals, "")
	c.Assert(a.SecurityType, Equals, enum.CommonStock)
	c.Assert(a.EffectiveDate, Equals, time.Time{})
	c.Assert(a.TargetCusip, Equals, "928534106")
	c.Assert(a.TargetDescription, Equals, "VIVIC CORP")
	c.Assert(a.InitiatingCusip, Equals, "999999999")
	c.Assert(a.InitiatingDescription, Equals, "9999999999")
	c.Assert(a.ExpirationDate, Equals, date(2020, time.January, 27))
	c.Assert(a.Cash, Equals, 0.0)
	c.Assert(a.CashCode, Equals, "")
	c.Assert(a.StateCode, Equals, "")
	c.Assert(a.RecordDate, Equals, date(2020, time.January, 20))
	c.Assert(a.Rate, Equals, 4.0)
	c.Assert(a.RateCode, Equals, "")
	c.Assert(a.VoluntaryMandatoryCode, Equals, enum.MandatoryAction)
	c.Assert(a.UpdateTextNumber, Equals, 0)
	c.Assert(a.DeleteTextNumber, Equals, 0)
	c.Assert(a.NewRate, Equals, 4.0)
	c.Assert(a.OldRate, Equals, 1.0)
	c.Assert(a.DueRedemptionDate, Equals, time.Time{})
}

func (t *TestAnnouncementSuite) TestReadRecords(c *C) {
	input := `........................................................... ENT:01/24/20:(2020) 
DEAL#: 1: TEXT#:2052815: REMARKS:EXPIRATION  :   IND:@UT1C: EFF:99/99/99:(9999) 
TARGET:26153M200: :DREAM UNLI: INIT:999999999: :DREAM UNLI: EXP:01/22/20:(2020) 
CASH:  11.75000:  :F: PROTECT:2 :  T.AGENT:    : INFO:    : WDR:01/22/20:(2020) 
RATE:   0.00000:  : : :  1.00000:V:DTC: : PRATE:  0.000000: PRO:01/22/20:(2020) 
OPT:01: "DREAM UNLIMITED CORP"                        UPDTEXT:2035174:          
ANNOUNCED THAT THE TENDER OFFER FOR UP TO 10,000,000 OF ITS COM CL A            
SUB VTG SHARES                                                                  
EXPIRED JANUARY 22, 2020                                                        
TERMS: FOR EACH COM CL A SUB VTG SHARE HOLDERS WILL RECEIVE $11.75              
(CDN) IN CASH:                                                                  
THE OFFER, WITHDRAWAL RIGHTS AND PRORATION PERIOD WILL EXPIRE AT:               
:TIME: :05:00: :PM: :EST: :WEDNESDAY, JANUARY 22, 2020                          
PROTECT PERIOD: TWO BUSINESS DAYS.                                              
NOTE: THE OFFER IS NOT CONDITIONAL UPON ANY MINIMUM NUMBER OF SHARES            
BEING TENDERED. THE OFFER IS SUBJECT TO POSSIBLE PRORATION. THERE ARE           
ODD-LOT PREFERENCES & CONDITIONAL TENDER PROVISIONS AVAILABLE. THE              
CONTINUED ON TEXT NO. 2052816, DATED 01-24-2020                    ***          
........................................................... ENT:01/24/20:(2020) 
DEAL#: 1: TEXT#:2052816: REMARKS:EXPIRATION  :   IND:@UT1C: EFF:99/99/99:(9999) 
TARGET:26153M200: :DREAM UNLI: INIT:999999999: :DREAM UNLI: EXP:01/22/20:(2020) 
CASH:  11.75000:  :F: PROTECT:2 :  T.AGENT:    : INFO:    : WDR:01/22/20:(2020) 
RATE:   0.00000:  : : :  1.00000:V:DTC: : PRATE:  0.000000: PRO:01/22/20:(2020) 
OPT:01: CONTINUATION OF TEXT NO. 2052815              UPDTEXT:2035174:          
TOTAL NUMBER OF SHARES ACCEPTED IN THE OFFER WERE 10,000,000. THE               
APPROXIMATE PRORATION FACTOR IS 81.1%. PLEASE REFER TO THE PROSPECTUS           
FOR FURTHER DETAIL CONDITIONS.:                                                 
DEPOSITARY: COMPUTERSHARE TRUST COMPANY OF CANADA                               
ADDRESS: 100 UNIVERSITY AVENUE 8TH FLOOR                                        
TORONTO, ON M5J 2Y1                                                             
TEL: 800-564-6253                                                               
[UPDATED PRORATION FACTOR & NOTES]                                  **          
........................................................... ENT:01/24/20:(2020) 
DEAL#: 1: TEXT#:2052817: REMARKS:INFORMATION :   IND:EUE1C: EFF:99/99/99:(9999) 
TARGET:11144V105: :BROADWAY G: INIT:999999999: :9999999999: EXP:99/99/99:(9999) 
CASH:   0.00000:  :N: PROTECT:  :  T.AGENT:    : INFO:    : WDR:99/99/99:(9999) 
RATE:   0.00000:  :N: :  1.00000:N:DTC: : PRATE:  0.000000: PRO:99/99/99:(9999) 
OPT:01: "BROADWAY GOLD MNG LTD"                       UPDTEXT:1984589:          
COM                                                                             
ANNOUNCED THAT ITS SHAREHOLDERS WILL MEET ON FEBRUARY 19, 2020 TO               
VOTE ON REVERSE STOCK SPLIT AND NAME CHANGE.                                    
THE EXCHANGE RATE WILL BE 1 NEW SHARE FOR 8 OLD SHARES.                         
THE NEW NAME WILL BE MIND MEDICINE INC.                                         
NOTE: ONLY HOLDERS OF RECORD AT THE CLOSE OF BUSINESS ON JANUARY 14,            
2020 WILL BE ENTITLED TO VOTE AT THE MEETING.:                                  
SHAREHOLDER MEETING DATE:02-19-2020:                                            
BROADWAY GOLD MNG LTD                                                           
TEL: 800 680-0661                                                               
[UPDATED SHAREHOLDER MEETING DATE AND NOTES]                        **          
**********************************************************************          
99999999999999999999999999999999999999999999999999999999999999999999999999999999
`
	var announcements = []Announcement{}
	readRecords(input, &announcements)

	c.Assert(len(announcements), Equals, 2)
	first := announcements[0]
	second := announcements[1]
	c.Assert(first.TextNumber, Equals, 2052815)
	c.Assert(first.TargetCusip, Equals, "26153M200")
	c.Assert(first.UpdateTextNumber, Equals, 2035174)
	c.Assert(second.TextNumber, Equals, 2052817)
	c.Assert(second.TargetCusip, Equals, "11144V105")
	c.Assert(second.UpdateTextNumber, Equals, 1984589)
}
