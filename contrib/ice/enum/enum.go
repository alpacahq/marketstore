package enum

const (
	ProcessedFlag   = ".processed"
	ReorgFilePrefix = "reorg"
	SirsFilePrefix  = "sirs"
	BucketkeySuffix = "/1D/ACTIONS"
)

const (
	Approval            = 'P'
	Bankruptcy          = '&'
	Completion          = 'C'
	Consent             = '('
	ConsentTender       = ')'
	Conversion          = 'L'
	CUSIPChange         = '='
	Default             = 'Z'
	Distribution        = '#'
	EscrowToMaturity    = '>'
	Exchange            = 'X'
	Exercise            = 'H'
	Expiration          = '@'
	Extension           = 'l'
	Information         = 'E'
	Liquidation         = 'K'
	Meeting             = 'G'
	Merger              = 'M'
	MergerElection      = '-'
	NameChange          = '$'
	NewOffer            = 'N'
	OddLotTender        = ';'
	OfferToBuy          = 'O'
	Prerefunded         = '!'
	Proration           = '5'
	PutMandatory        = 'I'
	PutOptional         = 'V'
	Recapitalization    = 'U'
	Reclassified        = '?'
	RedemptionFull      = '*'
	RedemptionPartial   = '2'
	Rejection           = 'R'
	ReverseStockSplit   = '+'
	RightsPlanAdoption  = '8'
	RightsPlanExecution = '%'
	SpinOff             = '9'
	StockDividend       = '/'
	StockSplit          = '7'
	TenderOffer         = 'T'
	Termination         = 'Q'
	UnitSplit           = 'S'
)

const (
	NewAnnouncement     = 'N'
	UpdatedAnnouncement = 'U'
	DeletedAnnouncement = 'D'
)

const (
	VoluntaryAction = 'V'
	MandatoryAction = 'M'
	NotApplicable   = 'N'
)

const (
	CommonStock                      = 'C'
	PreferredStock                   = 'P'
	Warrant                          = 'W'
	Unit                             = 'U'
	CorporateBond                    = 'B'
	MunicipalBond                    = 'M'
	GovernmentBond                   = 'G'
	Right                            = 'R'
	ShareOfBeneficialInterest        = 'S'
	AmericanDepositoryReceipt        = 'A'
	OrdinaryShare                    = 'O'
	CollateralizedMortgageObligation = 'X'
)
