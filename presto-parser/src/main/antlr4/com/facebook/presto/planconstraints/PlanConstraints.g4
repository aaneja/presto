grammar PlanConstraints;

// Lexer rules
WS         : [ \t\r\n]+ -> skip ;
NUMBER     : [0-9]+ ;
LOJ        : 'LOJ' ;
ROJ        : 'ROJ' ;
IJ         : 'IJ' ;
LPAREN     : '(' ;
RPAREN     : ')' ;
JOIN_DIST_PARTITIONED : '[P]' ;
JOIN_DIST_REPLICATED : '[R]' ;
CARD : 'CARD' ;
JOIN : 'JOIN' ;
CONSTRAINTS_START_MARKER :'/*!';
CONSTRAINTS_END_MARKER :'*/';

fragment DIGIT
    : [0-9]
    ;

fragment LETTER
    : [A-Z]
    | [a-z]
    ;

IDENTIFIER
    : (LETTER | '_') (LETTER | DIGIT | '_' | '@' | ':')*
    ;

identifier
    : IDENTIFIER
    ;

standAloneRelation
    : identifier
    ;

groupedRelation
    : LPAREN joinedRelation RPAREN
    ;

joinTypeOrDefault
    : joinType
    | { "IJ" } // Default value
    ;

joinType
    : LOJ
    | ROJ
    | IJ
    ;

joinAttribute
    : JOIN_DIST_PARTITIONED
    | JOIN_DIST_REPLICATED
    ;

joinedRelation
    : standAloneRelation joinTypeOrDefault standAloneRelation (joinAttribute)? #ss
    | standAloneRelation joinTypeOrDefault groupedRelation (joinAttribute)? #sg
    | groupedRelation joinTypeOrDefault standAloneRelation (joinAttribute)? #gs
    | groupedRelation joinTypeOrDefault groupedRelation (joinAttribute)? #gg
    ;


cardinalityConstraint
    : CARD LPAREN joinedRelation NUMBER RPAREN
    | CARD LPAREN standAloneRelation NUMBER RPAREN
    ;

joinConstraint : JOIN LPAREN joinedRelation RPAREN;

planConstraint
    : joinConstraint
    | cardinalityConstraint;

planConstraintString : CONSTRAINTS_START_MARKER (planConstraint)* CONSTRAINTS_END_MARKER;
