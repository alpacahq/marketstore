package enum

const (
	ProcessedFlag   = ".processed"
	ReorgFilePrefix = "reorg"
	SirsFilePrefix  = "sirs"
	BucketkeySuffix = "/1D/ACTIONS"
)

type NotificationType byte

const (
	Approval            NotificationType = 'P'
	Bankruptcy          NotificationType = '&'
	Completion          NotificationType = 'C'
	Consent             NotificationType = '('
	ConsentTender       NotificationType = ')'
	Conversion          NotificationType = 'L'
	CUSIPChange         NotificationType = '='
	Default             NotificationType = 'Z'
	Distribution        NotificationType = '#'
	EscrowToMaturity    NotificationType = '>'
	Exchange            NotificationType = 'X'
	Exercise            NotificationType = 'H'
	Expiration          NotificationType = '@'
	Extension           NotificationType = 'l'
	Information         NotificationType = 'E'
	Liquidation         NotificationType = 'K'
	Meeting             NotificationType = 'G'
	Merger              NotificationType = 'M'
	MergerElection      NotificationType = '-'
	NameChange          NotificationType = '$'
	NewOffer            NotificationType = 'N'
	OddLotTender        NotificationType = ';'
	OfferToBuy          NotificationType = 'O'
	Prerefunded         NotificationType = '!'
	Proration           NotificationType = '5'
	PutMandatory        NotificationType = 'I'
	PutOptional         NotificationType = 'V'
	Recapitalization    NotificationType = 'U'
	Reclassified        NotificationType = '?'
	RedemptionFull      NotificationType = '*'
	RedemptionPartial   NotificationType = '2'
	Rejection           NotificationType = 'R'
	ReverseStockSplit   NotificationType = '+'
	RightsPlanAdoption  NotificationType = '8'
	RightsPlanExecution NotificationType = '%'
	SpinOff             NotificationType = '9'
	StockDividend       NotificationType = '/'
	StockSplit          NotificationType = '7'
	TenderOffer         NotificationType = 'T'
	Termination         NotificationType = 'Q'
	UnitSplit           NotificationType = 'S'
	UnsetNotification   NotificationType = 0
)

type StatusCode byte

const (
	NewAnnouncement     StatusCode = 'N'
	UpdatedAnnouncement StatusCode = 'U'
	DeletedAnnouncement StatusCode = 'D'
)

type ActionCode byte

const (
	VoluntaryAction ActionCode = 'V'
	MandatoryAction ActionCode = 'M'
	NotApplicable   ActionCode = 'N'
)

type SecurityType byte

const (
	CommonStock                      SecurityType = 'C'
	PreferredStock                   SecurityType = 'P'
	Warrant                          SecurityType = 'W'
	Unit                             SecurityType = 'U'
	CorporateBond                    SecurityType = 'B'
	MunicipalBond                    SecurityType = 'M'
	GovernmentBond                   SecurityType = 'G'
	Right                            SecurityType = 'R'
	ShareOfBeneficialInterest        SecurityType = 'S'
	AmericanDepositoryReceipt        SecurityType = 'A'
	OrdinaryShare                    SecurityType = 'O'
	CollateralizedMortgageObligation SecurityType = 'X'
)
