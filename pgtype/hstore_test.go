package pgtype_test

import (
	"context"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxtest"
)

func isExpectedEqMapStringString(a any) func(any) bool {
	return func(v any) bool {
		am := a.(map[string]string)
		vm := v.(map[string]string)

		if len(am) != len(vm) {
			return false
		}

		for k, v := range am {
			if vm[k] != v {
				return false
			}
		}

		return true
	}
}

func isExpectedEqMapStringPointerString(a any) func(any) bool {
	return func(v any) bool {
		am := a.(map[string]*string)
		vm := v.(map[string]*string)

		if len(am) != len(vm) {
			return false
		}

		for k, v := range am {
			if (vm[k] == nil) != (v == nil) {
				return false
			}

			if v != nil && *vm[k] != *v {
				return false
			}
		}

		return true
	}
}

// stringPtr returns a pointer to s.
func stringPtr(s string) *string {
	return &s
}

func TestHstoreCodec(t *testing.T) {
	ctr := defaultConnTestRunner
	ctr.AfterConnect = func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		var hstoreOID uint32
		err := conn.QueryRow(context.Background(), `select oid from pg_type where typname = 'hstore'`).Scan(&hstoreOID)
		if err != nil {
			t.Skipf("Skipping: cannot find hstore OID")
		}

		conn.TypeMap().RegisterType(&pgtype.Type{Name: "hstore", OID: hstoreOID, Codec: pgtype.HstoreCodec{}})
	}

	tests := []pgxtest.ValueRoundTripTest{
		{
			map[string]string{},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{}),
		},
		{
			map[string]string{"foo": "", "bar": "", "baz": "123"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "", "bar": "", "baz": "123"}),
		},
		{
			map[string]string{"NULL": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"NULL": "bar"}),
		},
		{
			map[string]string{"bar": "NULL"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"bar": "NULL"}),
		},
		{
			map[string]string{"": "foo"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"": "foo"}),
		},
		{
			map[string]*string{},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{}),
		},
		{
			map[string]*string{"foo": stringPtr("bar"), "baq": stringPtr("quz")},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{"foo": stringPtr("bar"), "baq": stringPtr("quz")}),
		},
		{
			map[string]*string{"foo": nil, "baq": stringPtr("quz")},
			new(map[string]*string),
			isExpectedEqMapStringPointerString(map[string]*string{"foo": nil, "baq": stringPtr("quz")}),
		},
		{nil, new(*map[string]string), isExpectedEq((*map[string]string)(nil))},
		{nil, new(*map[string]*string), isExpectedEq((*map[string]*string)(nil))},
		{nil, new(*pgtype.Hstore), isExpectedEq((*pgtype.Hstore)(nil))},
	}

	specialStrings := []string{
		`"`,
		`'`,
		`\`,
		`\\`,
		`=>`,
		` `,
		`\ / / \\ => " ' " '`,
		"line1\nline2",
		"tab\tafter",
		"vtab\vafter",
		"form\\ffeed",
		"carriage\rreturn",
		"curly{}braces",
		// Postgres on Mac OS X hstore parsing bug:
		// ą = "\xc4\x85" in UTF-8; isspace(0x85) on Mac OS X returns true instead of false
		"mac_bugą",
	}
	for _, s := range specialStrings {
		// Special key values

		// at beginning
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{s + "foo": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s + "foo": "bar"}),
		})
		// in middle
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo" + s + "bar": "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s + "bar": "bar"}),
		})
		// at end
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo" + s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo" + s: "bar"}),
		})
		// is key
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{s: "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{s: "bar"}),
		})

		// Special value values

		// at beginning
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s + "bar"}),
		})
		// in middle
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": "foo" + s + "bar"},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s + "bar"}),
		})
		// at end
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": "foo" + s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": "foo" + s}),
		})
		// is key
		tests = append(tests, pgxtest.ValueRoundTripTest{
			map[string]string{"foo": s},
			new(map[string]string),
			isExpectedEqMapStringString(map[string]string{"foo": s}),
		})
	}

	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, pgxtest.KnownOIDQueryExecModes, "hstore", tests)

	// run the tests using pgtype.Hstore as input and output types, and test all query modes
	for i := range tests {
		var h pgtype.Hstore
		switch typedParam := tests[i].Param.(type) {
		case map[string]*string:
			h = pgtype.Hstore(typedParam)
		case map[string]string:
			if typedParam != nil {
				h = pgtype.Hstore{}
				for k, v := range typedParam {
					h[k] = stringPtr(v)
				}
			}
		}

		tests[i].Param = h
		tests[i].Result = &pgtype.Hstore{}
		tests[i].Test = func(input any) bool {
			return reflect.DeepEqual(input, h)
		}
	}
	pgxtest.RunValueRoundTripTests(context.Background(), t, ctr, pgxtest.AllQueryExecModes, "hstore", tests)

	// run the tests again without the codec registered: uses the text protocol
	ctrWithoutCodec := defaultConnTestRunner
	pgxtest.RunValueRoundTripTests(context.Background(), t, ctrWithoutCodec, pgxtest.AllQueryExecModes, "hstore", tests)

	// scan empty and NULL: should be different in all query modes
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()

	pgxtest.RunWithQueryExecModes(ctx, t, ctr, pgxtest.AllQueryExecModes, func(ctx context.Context, t testing.TB, conn *pgx.Conn) {
		h := pgtype.Hstore{"should_be_erased": nil}
		err := conn.QueryRow(ctx, `select cast(null as hstore)`).Scan(&h)
		if err != nil {
			t.Fatal(err)
		}
		expectedNil := pgtype.Hstore(nil)
		if !reflect.DeepEqual(h, expectedNil) {
			t.Errorf("plain conn.Scan failed expectedNil=%#v actual=%#v", expectedNil, h)
		}

		err = conn.QueryRow(ctx, `select cast('' as hstore)`).Scan(&h)
		if err != nil {
			t.Fatal(err)
		}
		expectedEmpty := pgtype.Hstore{}
		if !reflect.DeepEqual(h, expectedEmpty) {
			t.Errorf("plain conn.Scan failed expectedEmpty=%#v actual=%#v", expectedEmpty, h)
		}
	})
}

func TestParseInvalidInputs(t *testing.T) {
	// these inputs should be invalid, but previously were considered correct
	invalidInputs := []string{
		// extra comma between values
		`"a"=>"1", ,b"=>"2"`,
		// missing doublequote before second value
		`""=>"", 0"=>""`,
	}
	for i, input := range invalidInputs {
		var hstore pgtype.Hstore
		err := hstore.Scan(input)
		if err == nil {
			t.Errorf("test %d: input=%s (%#v) should fail; parsed correctly", i, input, input)
		}
	}
}

func TestRoundTrip(t *testing.T) {
	codecs := []struct {
		name       string
		encodePlan pgtype.EncodePlan
		scanPlan   pgtype.ScanPlan
	}{
		{
			"text",
			pgtype.HstoreCodec{}.PlanEncode(nil, 0, pgtype.TextFormatCode, pgtype.Hstore(nil)),
			pgtype.HstoreCodec{}.PlanScan(nil, 0, pgtype.TextFormatCode, (*pgtype.Hstore)(nil)),
		},
		{
			"binary",
			pgtype.HstoreCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, pgtype.Hstore(nil)),
			pgtype.HstoreCodec{}.PlanScan(nil, 0, pgtype.BinaryFormatCode, (*pgtype.Hstore)(nil)),
		},
	}

	inputs := []pgtype.Hstore{
		nil,
		{},
		{"": stringPtr("")},
		{"k1": stringPtr("v1")},
		{"k1": stringPtr("v1"), "k2": stringPtr("v2")},
	}
	for _, codec := range codecs {
		for i, input := range inputs {
			t.Run(fmt.Sprintf("%s/%d", codec.name, i), func(t *testing.T) {
				serialized, err := codec.encodePlan.Encode(input, nil)
				if err != nil {
					t.Fatal(err)
				}
				var output pgtype.Hstore
				err = codec.scanPlan.Scan(serialized, &output)
				if err != nil {
					t.Fatal(err)
				}
				if !reflect.DeepEqual(output, input) {
					t.Errorf("output=%#v does not match input=%#v", output, input)
				}
			})
		}
	}

}

func BenchmarkHstoreEncode(b *testing.B) {
	h := pgtype.Hstore{"a x": stringPtr("100"), "b": stringPtr("200"), "c": stringPtr("300"),
		"d": stringPtr("400"), "e": stringPtr("500")}

	serializeConfigs := []struct {
		name       string
		encodePlan pgtype.EncodePlan
	}{
		{"text", pgtype.HstoreCodec{}.PlanEncode(nil, 0, pgtype.TextFormatCode, h)},
		{"binary", pgtype.HstoreCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, h)},
	}

	for _, serializeConfig := range serializeConfigs {
		var buf []byte
		b.Run(serializeConfig.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				var err error
				buf, err = serializeConfig.encodePlan.Encode(h, buf)
				if err != nil {
					b.Fatal(err)
				}
				buf = buf[:0]
			}
		})
	}
}

func BenchmarkHstoreScan(b *testing.B) {
	// empty, NULL, escapes, and based on some real data
	benchStrings := []string{
		"",
		`"a"=>"b"`,
		`"a"=>"100", "b"=>"200", "c"=>"300", "d"=>"400", "e"=>"500"`,
		`"a"=>"100", "b"=>NULL, "c"=>"300", "d"=>NULL, "e"=>"500"`,
		`"pmd"=>"piokifjzxdy:mhvvmotns:sf1-dttudcp-orx-fuwzw-j8o-tl-jcg-1fb5d6dp50ke3l24", "ausz"=>"aorc-iosdby_tbxsjihj-kss64-32r128y-i2", "mgjo"=>"hxcp-ciag", "hkbee"=>"bokihheb", "gpcvhc"=>"ne-ywik-1", "olzjegk"=>"rxbkzba", "iy_quthhf"=>"sryizraxx", "bwpdpplfz"=>"gbdh-jikmnp_jwugdvjs-drh64-32k128h-p2", "njy_veipyyl"=>"727006795293", "vsgvqlrnqadzvk"=>"1_7_43", "mfdncuqvxp_gqlkytj"=>"fuyin", "cnuiswkwavoupqebov"=>"x32n128w", "mol_lcabioescln_ulstxauvi"=>"qm1-adbcand-tzi-fpnbv-s8j-vi-gqs-1om5b6lx50zk3u24", "arlyhgdxux.fc/bezucmz/mmfed"=>"vihsk", "jtkf.czddftrhr.ici/qbq_ftaz"=>"sse64", "notxkfqmpq.whxmykhtc.bcu/zmxz"=>"zauaklqp-uwo64-32q128a-g2", "ww_affdwqa_o8o_ilskcucq_urzltnf"=>"i6-9-0", "f8d.eq/bbqxwru-vsznvxerae/wsszbjw"=>"dgd", "ygpghkljze.dkrlrrieo.iur/xfqdqreft"=>"pfby-bhqlmm", "pmho-dqxuezyuu.ppslmznja.eam/ikehtxg"=>"wbku", "ckqeavtcqk.jiqdipgji.hjl/luzgqb-agm-wb"=>"ikpq", "akcn-yobdpxkyl.gktsjdo-xqwmivixku.p8y.vq/axqdw"=>"", "r8u.at/fbqrrss-ihxjmygoyc/ztqe-pqqqewnz/nepdj/njjv"=>"txtlffpp:ebwdksxkej", "q8x.wu/wenlhkz-govetdoibn/rcwg-ticalfjq/mgipy/awmjl"=>"dyzvbzvi", "p8l.wx/vadrnki-yfqhzlwcnt/hvun-geqhjsik/eqediipfr/vlc"=>"31900z", "t8z.be/qbtsmci-jqnqphssdg/ejma-slvywzry/txpnybwvn/kxdl"=>"210", "o8b.nb/bijgpwm-axvvqgujax/fjli-mxqwulfe/revyfoyty/oojpsd"=>"123421925786", "p8q.sk/ccpgzee-ufjempgvty/afwh-qvwzjvog/hsyhr/bklplujbfydtfw"=>"1_7_43", "k8y.jp/hqoymrw-flwqwvbntf/dlli-uggxkdqv/mtutu/qotjmacjitwtvcnblr"=>"m32x128f", "r8z.hj/eczodcw-lxzmeeqqii/fjba-psyoidht/gfjjcdbqs/apkqxiznu-muzubvl"=>"106068512341", "u8v.nf/ocnahkw-prhuwrrbjg/gxms-isohcouc/txfle/zfzw.neyygeeur.ejv/rnd_vdyo"=>"ibx64", "i8c.zz/dtiulqn-mmbskzjcib/fxuj-ejxbrnqi/optyp/wbbrancspv.pnkizgxcj.dbm/bldn"=>"znppnwzg-oxp64-32r128h-d2", "d8t.dg/jqtodoh-sokunyljow/svdf-ghplxxcx/wqkwl/dolljeqv.jcn.dxp.jmh.uyf/lyfv"=>"kc-lmpu-1i", "t8i.dy/imltbpr-atmthzarmk/fbbw-uaovyvdj/mmuwq/kseu-snmt.xtlgkstzph.mg/ehjdpgc"=>"", "o8c.yc/wximcpf-wmffadvnxx/tdim-szbqedqp/ztrui/puhx-kcwp.zziulqvvmb.ik/khfaxajj"=>"", "j8i.zc/sajavzi-kemnitliml/nloy-riqothpw/yxmnp/ttrnynffzy.lswpezbdq.wor/xkvqeexio"=>"ltmp-zajsxt", "a8f.xd/tfrrawy-ymihugugaa/ouzi-xdyecmqx/cwvgjvcrh/trgbxgbumo.uh/xmnqbds-nqxxeuqpq"=>"3123748065", "x8n.vx/juiqxkj-swvwogmncw/hvad-pojmevog/ytxit/auvo-duchssbth.uickilmnz.lja/hbeiakj"=>"hwhd", "z8j.bn/iplhrhv-wjdcwdclos/qndu-qvotchss/spvfx/brqotjnytw.aaemsoxor.ign/uwebjm-vzl-kb"=>"zwdg", "t8j.vx/iekvskm-xhikarvbty/czlm-xtipxwok/eeeow/uvtpuzmlqg.jgtpgiujc.wrs/mcofa-qxjjwak"=>"sovxb", "t8g.ab/wuncjdz-vsozsekgxz/aaea-hmgdjylm/qimwsoecgud-grgoowb/zveahbidvwcaebhlzigytiermehxy"=>"0.95", "n8k.ei/ohovibm-obkaatwlyw/bcow-gndyzpyt/aehyf/dpgifsorjx.ehsqntrka.jrr/meakdzy-ckxgnfavwm"=>"nlgw", "u8e.yi/qavbjew-qnmtzbeyce/rmwa-hcqlvadn/bhpml/taoj-wjnh.qqvkjmccfn.ja/nudbtwme-buc64-32j128i-k2"=>""`,
		`"mbgs"=>"eqjillclhkxz", "bxci"=>"etksm.rudiu", "jijqqm"=>"kj-ryxhwqtco-2", "yivvcxy"=>"fwbujcu", "ybk_ztlajai"=>"601427279990"`,
		`"wte"=>"nrhw", "lqjm"=>"ifsbygchn", "wbmf"=>"amjsoykkwq\\ghvwbsmz-qeiv-iekd-ukcwbipzy"`,
		`"otx"=>"fcreomqbwtk:gqhxzhxuh:wrqo-rf1-avhdpfy-nqi-dldof-i8p-mw-jll-l5r9741753c3", "vbjy"=>"akzfspigip_muzyxzwuso-zvoifh-uw", "fmkb"=>"pkoe-lezf", "wfbq"=>"qoviagajeg", "zvxbiv"=>"db-bcngmoq-1", "olictqnpx"=>"taqcnrcwcj_ticfxydekq-fafbkg-ot", "wkt_jtzzqpt"=>"727006795293", "bsdncvmbvj_xivgkws"=>"zczag", "muzq.oyrphhtne.fqm/itc"=>"ihilzgx", "pfsd.xphmjdohu.hrm/yeimpfm"=>"lrrqxrwyud-uvcljo", "qukdxappwo.or/xgcsmdo/dodoj"=>"onflq", "ktqrsqtllo.xxxpkizlg.tnf/unrt"=>"jrveutvddu-loihei-ww", "tr_qmarsis_s8v_skzbuuvy_cnyuxyk"=>"g6-16-0", "z8q.yc/xistcyy-tftbikuuhg/zvhemmi"=>"knv", "zrgwpjnvzq.twkcxxuyk.qwc/nirbacaom"=>"okfdlcpbdg", "suvk-wwwjqdytq.wdjmzxl-nduettmnmf.e8e.ec/qhkan"=>"", "u8m.xa/uvbhlmw-rqrcyyaiju/otsg-bqjfitoq/zqfuq/fifo"=>"brarmrogdb", "b8o.ci/znwkyby-nzuxiguqus/nwou-cxxnqxrr/rtdsp/yawv"=>"juedpptnbt-khocdt-vg:vfxpdswxnc", "u8h.vl/kgmvysr-xhykrjcssj/jfjv-gzalgika/yhrjfytwz/kbm"=>"3900f", "y8b.cm/ttijscl-rznjossaqw/kvto-gvnavnep/bwdqyuzgo/ozoi"=>"40", "p8j.pd/bnucngv-vnqufgvfqw/qshw-obnkmlfx/obczheyis/zzbsos"=>"7009zf", "p8y.fc/ejbndrq-aariupaovi/mrah-hmrhjcsv/lvrmfwwiz/uskogxfuw-zamygae"=>"18747532246", "y8m.oh/xzuhilr-wqmqqzcznb/pcox-idpxmhfj/yzsoj/qebkjaeymc.abqznnelq.gyd/osvb"=>"hsgxlccalq-eeybug-mx", "p8f.ay/tyntrss-nljxedfihd/grvy-znfykhlf/fjsqd/ffxaixyv.jie.bkg.zpd.kim/mgtc"=>"or-vrkdcxm-1i", "i8m.ms/jtykfbi-jdrqsqjdwt/ibaq-zmeuyznf/uczny/ufmj-zklt.omodkgubqw.ip/xztdevd"=>"", "k8m.ui/ymxurqo-kuhofnewjj/twex-iuwljutj/warlx/zptkdgqdpr.uhvqtrclx.ohj/bdkgsozkk"=>"zlgisdikac", "g8b.wk/vecudfr-pljllpgzxi/lbwd-zsracrgq/fucssaowj/syizbmlfqt.si/swpbend-gxrhddxad"=>"156213905", "z8y.ah/azeasta-gffxfwklrn/hukw-hphwntwy/lfswv/tmaeaxekya.vgkxjhtvg.mht/bzolt-koioxpf"=>"wzkra", "f8l.sy/ouekhco-rlhsclfzwx/erfz-uuejogrs/bgvia/zpohrhmrmu.sbdxzlaxo.wii/jbnwfvz-shekbewool"=>"aiey", "j8w.pz/fjtkxhn-zxxizfldde/wsik-uiodldga/ljdtl/gswz-cjmt.ffkelhxcsd.lw/ftcqgdnnho-ibbfql-ww"=>""`,
		`"uvd"=>"oneotg", "wsm"=>"djjgmwqyple:jtxtfvtjv:du1-nfxzmra-idl-ikxbx-t8n-id-nbo-6d08opx70381", "orq"=>"bkdvjw-xydgbd1", "gblm"=>"jtkcfd-unxbag1_xagyfw-nvachf1", "mfer"=>"jclz-yaim", "jvgvas"=>"jf-vhxh-1", "wwardeuqu"=>"ufimeb-bscfdy1_bfuagy-dhdqra1", "szs_rfgpqmc"=>"727006795293", "ckfxcgrnqc_rloxzxu"=>"qffbw", "yaigdvscju.ba/krpgzji/wvxyg"=>"srgtu", "gtxfjsigdv.pxujnffnp.aza/ycco"=>"ntranp-ahgeem1", "xj_lhdpvsl_i8i_qzrtlpjr_nroujqh"=>"q6-1-8", "czxy-sfym.enlohvvjmp.wb/huvcuhy"=>"", "x8a.of/sqpdqiq-vijrlgkkyl/oncckls"=>"mij", "oomgvfopmc.trnzktrtz.gza/rpeqqyqmm"=>"rgwnma-bwcbxe1", "gaud-giar.xuablvwkbo.wy/wvhmsk-uaycqn1"=>"", "oarbmcqzzw.qkfbtmltz.plh/aqssj-tlrhsof"=>"wxfd", "zepirccplb.qanvqnxlo.eld/emulnov-vgddsefeqv"=>"jnvh", "acby-kywxjuczc.suosfcy-drsgroeqvy.o8m.og/vyuxt"=>"", "q8j.by/lrwxbjt-yzrenlniog/gbmw-mnokcndu/etbcy/ibwr"=>"qpttug-jnxhwe1:grmslxhyky", "i8y.uy/awavkxk-nztmqujxys/pocu-sqjdqvzd/tfdjeflpn/xsj"=>"7900c", "z8g.ia/yzfdvta-ffkciorpfl/kmjc-fgcdomlv/snvhhbjil/nhvn"=>"45", "s8l.ky/dtvxoqu-lzfdnykmdh/wtdg-aktximmy/hofzkpzel/wtghso"=>"14837zg", "v8e.rq/uosznaz-drypoapgpe/vxss-mbxmvkjj/oglvxhxcz/whutvtjmr-tewtidr"=>"18747532246", "m8p.sz/hrgniti-aufhjdsdcc/whcp-cfuwjsnl/exugj/evphviokhl.ashpndixr.jvx/vgtt"=>"zdsacy-ppfuxf1", "w8t.fm/kljwjgc-fijbwsrvxa/dbzl-fhxvlrwk/yidyk/orrt-kgpr.wuzmpnxvtb.lc/dmbqfvt"=>"", "m8j.sv/takylmm-ywnolaflnl/ueih-fdcpfcpv/dslbc/dsspusnhtu.vgkihqtpb.fto/qmyksglfx"=>"wpwuih-deuiej1", "m8x.wi/jwobkio-mwupghbqbi/krqn-hqyfgwuw/mcbyi/yzkt-wtdy.pjxevrogab.tj/qlttbz-ppyzkd1"=>"", "c8j.tr/tzcbhid-lggaiypnny/wyms-zcjgxmwp/eaohd/bcwkheknsr.fqvtgecsf.qbf/uaqzj-jburpix"=>"ckkk", "w8h.wk/msbqvqy-nsmvbojwns/edpo-nsivbrmx/qifaf/sopuabsuvq.foyniwomd.zvj/lhvfwvv-zuufhhspso"=>"fghx"`,
		`"xxtlvd"=>"ba-zrzy-1", "hlebkcl"=>"entrcad", "ytn_toivqso"=>"601427279990", "czdllqyvkcfemhubpwvxakepubup"=>"jzhpff-vn2-sgiupfiii-qmuuz-ndex-vin-kmfm", "mefjcnjmcspgviisjalxmwdbksmge"=>"2022-11-20"`,
		`"ukq"=>"uhkbdj", "bmj"=>"mcoknsnhqcb:vmexvsccu:yt1-nscwdfr-zcp-ajfhr-z8i-ta-jhv-58yl03459t86", "cuq"=>"sqphbh-xkxbcgwdx", "dnac"=>"khzjpq-hljdvlbsw_azdisd-nshizhinc", "flgj"=>"zeem-pggu", "ksnn"=>"vpittgnl-xeojllby-toq", "wwxepg"=>"ki-cwee-1", "vigdnntxw"=>"sydsls-zidlsgugi_wviqvl-umwzyztab", "osz_utmlghi"=>"727006795293", "wacdaefqhc_buqmsci"=>"djtcv", "ljdbotgrsi.xn/gvtjfeg/iiyek"=>"lnfgg", "sohcclfodf.wkwiitult.ppm/hhsf"=>"ecpftm-ecmsibfjy", "dz_cgfnddq_o8j_cowdxlfz_rmjunpm"=>"v5-13-1", "niwk-fozq.tbamcxrhez.kl/zuxnisw"=>"", "h8k.xu/nbsezqz-fopcyqlnwt/lfcmgag"=>"dmm", "zebgpskksd.daigyeicb.dlj/dwmcpkohh"=>"hegecl-bnqmkunkl", "irjreiuove.qpmjixctw.mzv/xizjv-bpecdmy"=>"rkfl", "fupz-eiim.hwaqzvpzgv.yg/zhrqmr-qcydocyak"=>"", "djuscbflju.fmhnephvc.cmo/wzcisia-kqmrrhnkiv"=>"vchu", "hauo-olkeyvbrz.qzpaocu-wdbyfrzjkx.c8a.rn/bwhfe"=>"", "l8d.fj/jzojrmv-mbnxftbdzg/qvgo-oayrldze/tqmoa/oizo"=>"buwgyd-bjlrzrlci:ywosrfsnts", "q8l.sj/vifqvao-ynvfejvleb/ourc-jzridgtt/fgxnueuvm/wsg"=>"7900p", "d8b.mi/steijrv-bgajdbugff/kxkj-jhvctoxw/seyrafhni/xxrc"=>"45", "x8k.bn/dnnkttb-ywqrwwxirk/ngvt-eqyaeqsd/qesxmjfos/nlolbe"=>"14837xp", "v8o.az/vtbyyyo-rjuadsmwyb/gszv-ytnisfau/kfunvihsr/famkeacyo-skpueao"=>"18747532246", "f8w.ip/sjzrxbw-idgsgucprq/ster-zxiilwcf/luwzw/tavccuqfph.mcubdrtcr.ibw/dxnj"=>"ntyjnf-zwlyjqbfq", "y8f.mh/qykpkfr-fsnlckrhpe/hvyu-vstwrxkq/dmesn/kuor-acub.fqwqxcpiet.jf/zaxtdyb"=>"", "c8m.et/ekavnnp-gvpmldvoou/jzva-zzzpiecc/dvckb/qqxrfpoaiy.ssfqerwmb.cnz/odsfndorh"=>"liilkb-aekfuqzss", "e8n.gp/sybrxvz-mghjbpphpc/wcuo-naanbtcj/agtov/dztlgdacuz.fpbhhiybg.ncm/otgfu-hnezrwu"=>"ccez", "t8h.cy/bqsdiil-lxmioonwjt/drsw-qevzljvt/rvzjl/btbz-npvi.ypyxowgmfp.gf/jcfbyh-khpgbaayw"=>"", "y8b.df/anmudfn-gahfengbqw/fhdi-ozqtddmu/lvviu/kndwvowlby.jxkizwkac.hbq/fjkqyna-jijxahivma"=>"wxqg"`,
	}

	// convert benchStrings into text and binary bytes
	textBytes := make([][]byte, len(benchStrings))
	binaryBytes := make([][]byte, len(benchStrings))
	codec := pgtype.HstoreCodec{}.PlanEncode(nil, 0, pgtype.BinaryFormatCode, pgtype.Hstore(nil))
	for i, s := range benchStrings {
		textBytes[i] = []byte(s)

		var tempH pgtype.Hstore
		err := tempH.Scan(s)
		if err != nil {
			b.Fatal(err)
		}
		binaryBytes[i], err = codec.Encode(tempH, nil)
		if err != nil {
			b.Fatal(err)
		}
	}

	// benchmark the database/sql.Scan API
	var h pgtype.Hstore
	b.Run("databasesql.Scan", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			for _, str := range benchStrings {
				err := h.Scan(str)
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})

	// benchmark the []byte scan API used by pgconn
	scanConfigs := []struct {
		name       string
		scanPlan   pgtype.ScanPlan
		inputBytes [][]byte
	}{
		{"text", pgtype.HstoreCodec{}.PlanScan(nil, 0, pgtype.TextFormatCode, &h), textBytes},
		{"binary", pgtype.HstoreCodec{}.PlanScan(nil, 0, pgtype.BinaryFormatCode, &h), binaryBytes},
	}
	for _, scanConfig := range scanConfigs {
		b.Run(scanConfig.name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				for _, input := range scanConfig.inputBytes {
					err := scanConfig.scanPlan.Scan(input, &h)
					if err != nil {
						b.Fatalf("input=%#v err=%s", string(input), err)
					}
				}
			}
		})
	}
}
