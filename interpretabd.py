variables = {};

def declare(tree):
    if tree[1][1] == 'char':
        variables[tree[2][1]] = tree[3][1][1:len(tree[3][1]) - 1];
    else:
        variables[tree[2][1]] = tree[3][1];
    # print variables;

def dname(tree):
    if variables.has_key(tree[3][1]):
        x = variables[tree[3][1]];
        if tree[1][1] == 'int':
            if type(x) is int:
                variables[tree[2][1]] = variables[tree[3][1]];
            else:
                print 'ERROR: VARIABLE TYPE CHAR OF \'' + tree[3][1] + '\' DOES NOT MATCH INT TYPE OF VARIABLE ' + tree[2][1]
                exit();
        elif tree[1][1] == 'char':
            if type(x) is str:
                variables[tree[2][1]] = variables[tree[3][1]];
            else:
                print 'ERROR: VARIABLE TYPE INT OF \'' + tree[3][1] + '\' DOES NOT MATCH TYPE CHAR OF VARIABLE ' + tree[2][1]
                exit();

    else:
        print 'ERROR: VARIABLE ' + tree[3][1] + ' NOT DECLARED'
        exit();
    print variables;


def function(tree):
    if tree[0] == 'COUT':
        if tree[1] == 'STRING':
            print tree[2][1:(len(tree[2]) - 1)];
        elif tree[1] == 'NAME':
            if variables.has_key(tree[2]):
                print variables[tree[2]]
            else:
                print 'ERROR: VARIABLE ' + tree[2] + ' NOT DECLARED'
                exit();

def execute(tree):
    for x in tree:
        if x[0] == 'DECLARATION':
            declare(x);
        if x[0] == 'COUT':
            function(x);


def decx(treex):

    tree = treex[3][1];
    if variables.has_key(tree[2]):
        if tree[5] == 'NAME':
            if variables.has_key(tree[6]):
                x = variables[tree[2]];
                y = variables[tree[6]];
                if (type(x) is int) & (type(y) is int):
                    variables[treex[2][1]] = x + y;
                else:
                    print 'ERROR: VARIABLES ' + tree[2] + ' AND ' + tree[6] + ' DONT HAVE THE SAME TYPE'
            else:
                print 'ERROR: VARIABLE ' + tree[6] + ' NOT DECLARED'
                exit();
        else:
            x = variables[tree[2]];
            if (type(x) is int):
                variables[treex[2][1]] = x + tree[6];
            else:
                print 'ERROR: VARIABLE ' + tree[2] + ' DOES NOT HAVE TYPE INT';
                exit();
    else:
        print 'ERROR: VARIABLE ' + tree[2] + ' NOT DECLARED'
        exit();


    # print variables;
def case(tree):
    aller = 2;
    for x in tree:
        # print x
        if x[0] == 'CASE':
            # print x[1]
            # print variables['SWITCH']
            if x[1] == variables['SWITCH']:
                # print 'pppppppppppppp'
                if x[3] == 'BREAK':
                    # execute(x[2]);
                    start_prog(x[2]);
                    aller = 0;
                else:
                    execute(x[2]);
                    start_prog(x[2]);
                    aller = 1;
            elif aller == 1:
                # execute(x[2])
                start_prog(x[2]);
                if x[3] == 'BREAK':
                    aller = 0;
        if x[0] == 'DEFAULT':
            if aller >= 1:
                # print x[2]
                # print 'rtyui'
                # print x
                # execute(x[1]);
                start_prog(x[1]);
                return

def swtch(tree):
    if tree[1] == 'NAME':
        pass
        if variables.has_key(tree[2]):
            x = variables[tree[2]]
            if type(x) is int:
                variables[tree[0]] = variables[tree[2]]
            else:
                variables[tree[0]] = '\'' + variables[tree[2]] + '\'';
            # print variables;
        else:
            print 'ERROR: VARIABLE \'' + tree[2] + '\' NOT DECLARED'
            exit();
    elif tree[1] == 'CHAR':
        variables[tree[0]] = tree[2];
        # print variables
    else:
        variables[tree[0]] = tree[2];
        # print variables
    # print tree[3]
    case(tree[3])


def modme(tree):
    if variables.has_key(tree[2]):
        if tree[5] == 'NAME':
            if variables.has_key(tree[6]):
                x = variables[tree[2]];
                y = variables[tree[6]];
                if (type(x) is int) & (type(y) is int):
                    variables[tree[2]] = x + y;
                else:
                    print 'ERROR: VARIABLES ' + tree[2] + ' AND ' + tree[6] + ' DONT HAVE THE SAME TYPE'
            else:
                print 'ERROR: VARIABLE ' + tree[6] + ' NOT DECLARED'
                exit();
        else:
            x = variables[tree[2]];
            if (type(x) is int):
                variables[tree[2]] = x + tree[6];
                print variables;
            else:
                print 'ERROR: VARIABLE ' + tree[2] + ' DOES NOT HAVE TYPE INT';
                exit();


    else:
        print 'ERROR: VARIABLE ' + tree[2] + ' NOT DECLARED'
        exit();


def start_prog(tree):
    for x in tree:
        if x[0] == 'DECLARATION':
            declare(x);
        if x[0] == 'COUT':
            function(x);
        if x[0] == 'SWITCH':
            swtch(x);
        if x[0] == 'DECLARATION-N':
            dname(x);
        if x[0] == 'DECLARATIONX':
            decx(x);
        if x[0] == 'MODIFIER':
            modme(x);