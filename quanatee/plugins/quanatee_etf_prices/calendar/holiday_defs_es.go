package cal

import "time"

// Holidays in Spain
var (
	ESAñoNuevo               = NewYear
	ESReyes                  = NewHoliday(time.January, 6)
	ESFiestaDelTrabajo       = NewHoliday(time.May, 1)
	ESAsuncionDeLaVirgen     = NewHoliday(time.August, 15)
	ESFiestaNacionalDeEspaña = NewHoliday(time.October, 12)
	ESTodosLosSantos         = NewHoliday(time.November, 1)
	ESConstitucion           = NewHoliday(time.December, 6)
	ESInmaculadaConcepcion   = NewHoliday(time.December, 8)
	ESNavidad                = Christmas
)

// AddSpainHolidays adds all Spain holidays to the Calendar
func AddSpainHolidays(c *Calendar) {
	c.AddHoliday(
		ESAñoNuevo,
		ESReyes,
		ESFiestaDelTrabajo,
		ESAsuncionDeLaVirgen,
		ESFiestaNacionalDeEspaña,
		ESTodosLosSantos,
		ESConstitucion,
		ESInmaculadaConcepcion,
		ESNavidad,
	)
}
