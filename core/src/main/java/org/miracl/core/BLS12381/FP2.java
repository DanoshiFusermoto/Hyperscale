/*
 * Copyright (c) 2012-2020 MIRACL UK Ltd.
 *
 * This file is part of MIRACL Core
 * (see https://github.com/miracl/core).
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* Finite Field arithmetic  Fp^2 functions */

/* FP2 elements are of the form a+ib, where i is sqrt(-1) */

package org.miracl.core.BLS12381;
import org.miracl.core.RAND;

public final class FP2 
{
	// Working Variables //
	private static final ThreadLocal<BIG[]> mulBIG_3 = ThreadLocal.withInitial(() -> new BIG[] {new BIG(0), new BIG(0), new BIG(0)});
	private static final ThreadLocal<DBIG[]> mulDBIG_5 = ThreadLocal.withInitial(() -> new DBIG[] {new DBIG(0), new DBIG(0), new DBIG(0), new DBIG(0), new DBIG(0)});
	private static final ThreadLocal<FP[]> negFP_2 = ThreadLocal.withInitial(() -> new FP[] {new FP(0), new FP(0)});
	private static final ThreadLocal<FP[]> sqrFP_3 = ThreadLocal.withInitial(() -> new FP[] {new FP(0), new FP(0), new FP(0)});
	private static final ThreadLocal<FP2> subFP2 = ThreadLocal.withInitial(() -> new FP2(0));
	private static final ThreadLocal<FP2> mulipFP2 = ThreadLocal.withInitial(() -> new FP2(0));
	private static final ThreadLocal<FP2> qrFP2 = ThreadLocal.withInitial(() -> new FP2(0));
	private static final ThreadLocal<FP>  timesiFP = ThreadLocal.withInitial(() -> new FP(0));

	public static final FP2 ONE = new FP2(1);
	public static final FP2 ROM_CURVE_ADRI = new FP2(BIG.ROM_CURVE_Adr, BIG.ROM_CURVE_Adi);
	public static final FP2 ROM_CURVE_BDRI = new FP2(BIG.ROM_CURVE_Bdr, BIG.ROM_CURVE_Bdi); 
	public static final FP2 ROM_CURVE_RIAD = new FP2(CONFIG_FIELD.RIADZG2A,CONFIG_FIELD.RIADZG2B);
	public static final FP2 ROM_PCR_PCI[];
	static
	{
		ROM_PCR_PCI = new FP2[ROM.PCR.length];
		for (int i = 0 ; i < ROM.PCR.length ; i++)
			ROM_PCR_PCI[i] = new FP2(new BIG(ROM.PCR[i]), new BIG(ROM.PCI[i]));
	}

	private final FP a;
	private final FP b;

/* reduce components mod Modulus */
	public void reduce()
	{
		a.reduce();
		b.reduce();
	}

/* normalise components of w */
	public void norm()
	{
		a.norm();
		b.norm();
	}

/* test this=0 ? */
	public boolean iszilch() 
	{
		return (a.iszilch() && b.iszilch());
	}


    public int islarger() {
        if (iszilch()) return 0;
        int cmp=b.islarger();
        if (cmp!=0) return cmp;
        return a.islarger();
    }

    public void toBytes(byte[] bf) {
		byte[] t=new byte[CONFIG_BIG.MODBYTES];
        b.toBytes(t);
		for (int i=0;i<CONFIG_BIG.MODBYTES;i++)
            bf[i]=t[i];
        a.toBytes(t);
		for (int i=0;i<CONFIG_BIG.MODBYTES;i++)
            bf[i+CONFIG_BIG.MODBYTES]=t[i];
    }

    public static FP2 fromBytes(byte[] bf) {
		byte[] t=new byte[CONFIG_BIG.MODBYTES];
		for (int i=0;i<CONFIG_BIG.MODBYTES;i++)
            t[i]=bf[i];
        FP tb=FP.fromBytes(t);
		for (int i=0;i<CONFIG_BIG.MODBYTES;i++)
            t[i]=bf[i+CONFIG_BIG.MODBYTES];
        FP ta=FP.fromBytes(t);
        return new FP2(ta,tb);
    }

	public void cmove(FP2 g,int d)
	{
		a.cmove(g.a,d);
		b.cmove(g.b,d);
	}

/* test this=1 ? */
	public boolean isunity() 
	{
		return (a.equals(FP.ONE) && b.iszilch());
	}

/* test this=x */
	public boolean equals(FP2 x) 
	{
		return (a.equals(x.a) && b.equals(x.b));
	}

/* Constructors */
	public FP2()
	{
		a=new FP();
		b=new FP();
	}

	public FP2(int c)
	{
		a=new FP(c);
		b=new FP();
	}

	public FP2(int c,int d)
	{
		a=new FP(c);
		b=new FP(d);
	}

	public FP2(FP2 x)
	{
		a=new FP(x.a);
		b=new FP(x.b);
	}

	public FP2(FP c,FP d)
	{
		a=new FP(c);
		b=new FP(d);
	}

	public FP2(BIG c,BIG d)
	{
		a=new FP(c);
		b=new FP(d);
	}

	public FP2(FP c)
	{
		a=new FP(c);
		b=new FP();
	}

	public FP2(BIG c)
	{
		a=new FP(c);
		b=new FP();
	}

    public FP2(RAND rng)
    {
        a=new FP(rng);
        b=new FP(rng);
    }

/* extract a */
	public BIG getA()
	{ 
		return a.redc();
	}

/* extract b */
	public BIG getB()
	{
		return b.redc();
	}

/* extract a */
	public FP geta()
	{ 
		return a;
	}

/* extract b */
	public FP getb()
	{
		return b;
	}

/* copy this=x */
	public void copy(FP2 x)
	{
		a.copy(x.a);
		b.copy(x.b);
	}
	
	public void copy(FP x)
	{
		a.copy(x);
		b.zero();
	}

/* set this=0 */
	public void zero()
	{
		a.zero();
		b.zero();
	}

/* set this=1 */
	public void one()
	{
		a.one();
		b.zero();
	}
/* get sign */ 
    public int sign()
    {
        int p1=a.sign();
        int p2=b.sign();
        if (CONFIG_FIELD.BIG_ENDIAN_SIGN)
        {
            int u=b.iszilch()? 1:0;
            p2^=(p1^p2)&u;
            return p2;
        } else {
            int u=a.iszilch()? 1:0;
            p1^=(p1^p2)&u;
            return p1;
        }
    }

/* negate this mod Modulus */
	public void neg()
	{
		FP[] FP_2 = negFP_2.get();
		FP m=FP_2[0]; m.copy(a);
		FP t=FP_2[1];

		m.add(b);
		m.neg();
		t.copy(m); t.add(b);
		b.copy(m);
		b.add(a);
		a.copy(t);
	}

/* set to a-ib */
	public void conj()
	{
		b.neg();
		b.norm();
	}

/* this+=a */
	public void add(FP2 x)
	{
		a.add(x.a);
		b.add(x.b);
	}

/* this-=a */
	public void sub(FP2 x)
	{
		FP2 m=subFP2.get(); 
		m.copy(x);
		m.neg();
		add(m);
	}

	public void rsub(FP2 x)       // *****
	{
		neg();
		add(x);
	}

/* this*=s, where s is an FP */
	public void pmul(FP s)
	{
		a.mul(s);
		b.mul(s);
	}

/* this*=i, where i is an int */
	public void imul(int c)
	{
		a.imul(c);
		b.imul(c);
	}

/* this*=this */
	public void sqr()
	{
		FP[] FP_3 = sqrFP_3.get();
		FP w1=FP_3[0]; w1.copy(a);
		FP w3=FP_3[1]; w3.copy(a);
		FP mb=FP_3[2]; mb.copy(b);

		w1.add(b);
		mb.neg();

		w3.add(a);
		w3.norm();
		b.mul(w3);

		a.add(mb);

		w1.norm();
		a.norm();

		a.mul(w1);
	}

/* this*=y */
/* Now uses Lazy reduction */
	public void mul(FP2 y)
	{
		if ((long)(a.XES+b.XES)*(y.a.XES+y.b.XES)>(long)CONFIG_FIELD.FEXCESS)
		{
			if (a.XES>1) a.reduce();
			if (b.XES>1) b.reduce();		
		}

		BIG[] BIG_3 = mulBIG_3.get();
		DBIG[] DBIG_5 = mulDBIG_5.get();
		
		BIG C=BIG_3[0]; C.copy(a.x);
		BIG D=BIG_3[1]; D.copy(y.a.x);

		DBIG pR=DBIG_5[0]; pR.ucopy(BIG.ROM_MODULUS);
		DBIG A=DBIG_5[1]; BIG.mul(a.x, y.a.x, A);
		DBIG B=DBIG_5[2]; BIG.mul(b.x, y.b.x, B);

		C.add(b.x); C.norm();
		D.add(y.b.x); D.norm();

		DBIG E=DBIG_5[3]; BIG.mul(C,D,E);
		DBIG F=DBIG_5[4]; F.copy(A); F.add(B);
		B.rsub(pR);

		A.add(B); A.norm();
		E.sub(F); E.norm();

		BIG O=BIG_3[2];
		a.x.copy(FP.mod(A, O)); a.XES=3;
		b.x.copy(FP.mod(E, O)); b.XES=2;
	}
/*
    public void pow(BIG b)
    {
        FP2 w = new FP2(this);
        FP2 r = new FP2(1);
        BIG z = new BIG(b);
        while (true)
        {
            int bt = z.parity();
            z.shr(1);
            if (bt == 1) r.mul(w);
            if (z.iszilch()) break;
            w.sqr();
        }
        r.reduce();
        copy(r);
    }
*/
    public int qr(FP h)
    {
        FP2 c = qrFP2.get(); c.copy(this);
        c.conj();
        c.mul(this);

        return c.geta().qr(h);
    }

/* sqrt(a+ib) = sqrt(a+sqrt(a*a-n*b*b)/2)+ib/(2*sqrt(a+sqrt(a*a-n*b*b)/2)) */
	public void sqrt(FP h)
	{
		if (iszilch()) return;
		FP w1=new FP(b);
		FP w2=new FP(a);
		FP w3=new FP(a);
        FP w4=new FP();
        FP hint=new FP();

		w1.sqr(); w2.sqr(); w1.add(w2); w1.norm();
		
		w1=w1.sqrt(h);
		
        w2.copy(a); w2.add(w1); 
		w2.norm(); w2.div2();

        w1.copy(b); w1.div2();
        int qr=w2.qr(hint);

// tweak hint
        w3.copy(hint); w3.neg(); w3.norm();
        w4.copy(w2); w4.neg(); w4.norm();

        w2.cmove(w4,1-qr);
        hint.cmove(w3,1-qr);

        a.copy(w2.sqrt(hint));
        w3.copy(w2); w3.inverse(hint);
        w3.mul(a);
        b.copy(w3); b.mul(w1);
        w4.copy(a);

        a.cmove(b,1-qr);
        b.cmove(w4,1-qr);




/*

        a.copy(w2.sqrt(hint));
        w3.copy(w2); w3.inverse(hint);
        w3.mul(a);
        b.copy(w3); b.mul(w1);

        hint.neg(); hint.norm();
        w2.neg(); w2.norm();

        w4.copy(w2.sqrt(hint));
        w3.copy(w2); w3.inverse(hint);
        w3.mul(w4);
        w3.mul(w1);

        a.cmove(w3,1-qr);
        b.cmove(w4,1-qr);
*/
        int sgn=this.sign();
        FP2 nr=new FP2(this);
        nr.neg(); nr.norm();
        this.cmove(nr,sgn);
	}

/* output to hex string */
	public String toString() 
	{
		return ("["+a.toString()+","+b.toString()+"]");
	}

	public String toRawString() 
	{
		return ("["+a.toRawString()+","+b.toRawString()+"]");
	}

/* this=1/this */
	public void inverse(FP h)
	{
		norm();
		FP w1=new FP(a);
		FP w2=new FP(b);

		w1.sqr();
		w2.sqr();
		w1.add(w2);
		w1.inverse(h);
		a.mul(w1);
		w1.neg();
		w1.norm();
		b.mul(w1);
	}

/* this/=2 */
	public void div2()
	{
		a.div2();
		b.div2();
	}

/* this*=sqrt(-1) */
	public void times_i()
	{
		FP z=timesiFP.get(); z.copy(a);
		a.copy(b); a.neg();
		b.copy(z);
	}

/* w*=(2^i+sqrt(-1)) */
/* where X*2-(2^i+sqrt(-1)) is irreducible for FP4 */
	public void mul_ip()
	{
		FP2 t=mulipFP2.get(); t.copy(this);
		int i=CONFIG_FIELD.QNRI;
		times_i();
		while (i>0)
		{
			t.add(t);
			t.norm();
			i--;
		}
		add(t);
		if (CONFIG_FIELD.TOWER==CONFIG_FIELD.POSITOWER) {
			norm();
			neg();
		}

	}

/* w/=(2^i+sqrt(-1)) */
	public void div_ip()
	{
		FP2 z=new FP2(1<<CONFIG_FIELD.QNRI,1);
		z.inverse(null);
		norm();
		mul(z);
		if (CONFIG_FIELD.TOWER==CONFIG_FIELD.POSITOWER) {
			neg();
			norm();
		}
	}

}