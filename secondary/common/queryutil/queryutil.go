package queryutil

import (
	"errors"

	"github.com/couchbase/indexing/secondary/logging"
	qexpr "github.com/couchbase/query/expression"
	qparser "github.com/couchbase/query/expression/parser"
)

func IsArrayExpression(exp string) (bool, bool, bool, error) {
	cExpr, err := qparser.Parse(exp)
	if err != nil {
		return false, false, false, err
	}

	expr := cExpr.(qexpr.Expression)
	isArray, isDistinct, isFlatten := expr.IsArrayIndexKey()
	return isArray, isDistinct, isFlatten, nil
}

func NumFlattenKeys(exp string) (int, error) {
	cExpr, err := qparser.Parse(exp)
	if err != nil {
		return 0, err
	}

	if all, ok := cExpr.(*qexpr.All); ok && all.Flatten() {
		fk := all.FlattenKeys()
		return len(fk.Operands()), nil
	}

	return 0, errors.New("Invalid flatten expression")
}

func GetArrayExpressionPosition(exprs []string) (bool, bool, bool, int, error) {
	isArrayIndex := false
	isArrayDistinct := true   // Default is true as we do not yet support duplicate entries
	isArrayFlattened := false // Default is false as user has to explicitly specify "FLATTEN" keyword for array flattening
	arrayExprPos := -1
	for i, exp := range exprs {
		isArray, isDistinct, isFlatten, err := IsArrayExpression(exp)
		if err != nil {
			return false, false, false, -1, err
		}
		if isArray == true {
			isArrayIndex = isArray
			isArrayDistinct = isDistinct
			isArrayFlattened = isFlatten
			arrayExprPos = i
			break
		}
	}
	return isArrayIndex, isArrayDistinct, isArrayFlattened, arrayExprPos, nil
}

func GetXATTRNames(exprs []string) (present bool, names []string, err error) {
	parsedExprs := make([]qexpr.Expression, 0)
	xattrs := qexpr.NewField(qexpr.NewMeta(), qexpr.NewFieldName("xattrs", false))
	for _, expr := range exprs {
		pExpr, err := qparser.Parse(expr)
		if err != nil {
			return false, nil, err
		}
		if pExpr.EquivalentTo(xattrs) {
			return false, nil, errors.New("Fails to create index.  Can index only on a specific Extended Attribute.")
		}
		parsedExprs = append(parsedExprs, pExpr)
	}
	present, names = qexpr.XattrsNames(parsedExprs, "")
	return present, names, nil
}

// IsPartnKeyDocId returns true if there is only one partition key
// for an index which is either meta().id or its variant like
// meta(self).id. If there is more than on partition key (or)
// if the partition key is not based on meta().id (or its variants)
// then this method returns false
func IsPartnKeyDocId(partnKeys []string) bool {
	if len(partnKeys) != 1 {
		return false
	}

	partnExpr, err := qparser.Parse(partnKeys[0])
	if err != nil {
		logging.Fatalf("IsPartnKeyDocId: Fail to parse partition key", logging.TagUD(partnKeys[0]))
		return false
	}

	id := qexpr.NewField(qexpr.NewMeta(), qexpr.NewFieldName("id", false))
	idself := qexpr.NewField(qexpr.NewMeta(qexpr.NewSelf()), qexpr.NewFieldName("id", false))

	if partnExpr.EquivalentTo(id) || partnExpr.EquivalentTo(idself) {
		return true
	}
	return false
}
