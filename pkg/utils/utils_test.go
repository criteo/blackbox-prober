package utils_test

import (
	"regexp"
	"testing"

	"github.com/criteo/blackbox-prober/pkg/utils"
)

func TestContainsWorks(t *testing.T) {
	s := []string{"titi", "toto", "tutu", "tatata"}
	if !utils.Contains(s, "titi") ||
		utils.Contains(s, "tata") {
		t.Errorf("Contains doesn't work")
	}
}

func TestRandomHexWorks(t *testing.T) {
	re, _ := regexp.Compile("^[a-f0-9]{77}$")
	hex := utils.RandomHex(77)
	if !re.MatchString(hex) {
		t.Errorf("RandomHex doesn't work, generated hex: %s", hex)
	}

}
